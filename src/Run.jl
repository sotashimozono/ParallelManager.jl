# Run — the facade that ties a pure work_fn to Vault, KeyLock, Manifest,
# and EventLog. This is the public entry point most users will call after
# `init_workers!`.

using DataVault
using ParamIO: DataKey, canonical

"""
    RunOpts(; workers=:auto, max_attempts=3, stale_after=600.0,
             heartbeat_interval=60.0)

Execution options for [`run!`](@ref).

# Fields

- `workers::Symbol = :auto` — reserved for future dispatch. Currently
  [`run!`](@ref) iterates sequentially and relies on the caller having
  already invoked [`init_workers!`](@ref) as needed.
- `max_attempts::Int = 3` — per-key retry budget. Set to `1` to disable
  retry (a failed `work_fn` is logged as `:error` instead of `:gave_up`).
- `stale_after::Float64 = 600.0` — seconds before another master can
  reclaim a held lock as stale. Passed through to [`KeyLock`](@ref).
- `heartbeat_interval::Float64 = 60.0` — how often the per-lock heartbeat
  task refreshes its `heartbeat` file. Must be `<` `stale_after`.

# Example

```julia
opts = RunOpts(max_attempts=5, stale_after=900.0, heartbeat_interval=30.0)
ParallelManager.run!(work_fn, vault, keys; opts)
```
"""
struct RunOpts
    workers::Symbol
    max_attempts::Int
    stale_after::Float64
    heartbeat_interval::Float64
end

function RunOpts(;
    workers::Symbol=:auto,
    max_attempts::Int=3,
    stale_after::Real=600.0,
    heartbeat_interval::Real=60.0,
)
    RunOpts(workers, max_attempts, Float64(stale_after), Float64(heartbeat_interval))
end

# Internal: per-(vault.run, key) lock directory path, keyed by
# `canonical(key)` so multiple masters on the same vault agree on the
# location without touching DataVault internals. Lives under the vault's
# outdir so cleanup falls out of `rm -rf out/`.
function _key_lock_dir(vault::Vault, key::DataKey)
    joinpath(
        vault.outdir, "locks", vault.spec.study.project_name, vault.run, canonical(key)
    )
end

"""
    manifest_root(vault) -> String

Return the directory under which [`run!`](@ref) and [`load_manifest`](@ref)
look for this vault's `manifest.jld2` — one manifest per `(project, run)`.

The layout is:

    <vault.outdir>/manifest/<project_name>/<vault.run>/manifest.jld2

Pure function; does not touch the filesystem.
"""
function manifest_root(vault::Vault)
    joinpath(vault.outdir, "manifest", vault.spec.study.project_name)
end

"""
    load_manifest(vault::DataVault.Vault) -> Manifest

Convenience overload of the two-argument [`load_manifest`](@ref) that
derives `(root, stage)` from a `DataVault.Vault`:

    load_manifest(manifest_root(vault), Symbol(vault.run))
"""
load_manifest(vault::Vault) = load_manifest(manifest_root(vault), Symbol(vault.run))

"""
    run!(work_fn, vault, keys; opts=RunOpts()) -> NamedTuple

Run `work_fn(key) -> Dict` for every `key` in `keys`, persisting through
`vault`. This is the top-level entry point most users call.

# Pipeline

For each call, in order:

1. Open an [`EventLog`](@ref) at `joinpath(vault.outdir, "events.jsonl")`.
2. [`load_manifest`](@ref) and compute `todo = todo_keys(manifest, keys)`.
   If `todo` is empty, emit `:skip_complete` and return in ~O(1) time
   (痛点 #6 answer: full-done re-run is a single JLD2 read).
3. Emit `:stage_start` with `total` and `todo` counts.
4. For each key in `todo`:
   - Acquire a [`KeyLock`](@ref) under `vault.outdir/locks/...`. If another
     master holds it, emit `:lock_busy` and move on.
   - Inside the lock, re-check `DataVault.is_done(vault, key)` — another
     master may have finished this key between our manifest read and lock
     acquisition. If so, add it to our local manifest without running.
   - Call `DataVault.mark_running!` then dispatch to
     [`_run_one_with_retry!`](@ref), which will call `work_fn(key)` up to
     `opts.max_attempts` times. On success, `DataVault.save!` +
     `DataVault.mark_done!` are called and the key is added to the manifest.
5. [`save_manifest`](@ref) and emit `:stage_done` with the aggregate counts.

# Contract

- `work_fn` **must be pure**: given a `DataKey`, return a `Dict` payload.
  No IO, no globals, no logging. All of that lives in the runtime. Payloads
  that are not `Dict` are rejected as errors (this is a `DataVault.save!`
  constraint, not a stylistic one).
- An exception in `work_fn` does **not** mark the key done, so the next
  `run!` picks it up. After `max_attempts` failures, `:gave_up` is logged
  and the key is left unmarked in the manifest.
- The stage label used throughout events is `Symbol(vault.run)`.
- The manifest is **monotonic** — it is saved once at the end of the stage
  with every newly completed key. Crashing mid-stage loses the in-memory
  completions since the previous save, but the per-key `.done` files
  written by `DataVault.mark_done!` are authoritative on the next run.

# Parallel execution

Today `run!` iterates sequentially through `todo`; parallelism inside one
master comes from running multiple masters concurrently (each with its
own `run!` call). The [`KeyLock`](@ref) layer makes that safe.

# Return value

A `NamedTuple` with fields:

| Field     | Meaning                                             |
| :-------- | :-------------------------------------------------- |
| `stage`   | `Symbol(vault.run)`                                 |
| `done`    | keys whose `work_fn` succeeded this call           |
| `err`     | keys that failed this call (incl. `gave_up`)       |
| `busy`    | keys we skipped because another master held the lock |
| `gave_up` | keys that exhausted `max_attempts`                 |
| `skipped` | keys already complete (manifest or post-lock check) |
| `total`   | `length(keys)`                                     |

A pure-early-skip call returns
`(stage=..., done=0, err=0, skipped=length(keys), total=length(keys))`.

# Example

```julia
result = ParallelManager.run!(work_fn, vault, keys;
                              opts=RunOpts(max_attempts=5))
@info "stage complete" result
```
"""
function run!(
    work_fn::Function, vault::Vault, keys::AbstractVector{DataKey}; opts::RunOpts=RunOpts()
)
    stage = Symbol(vault.run)
    log = EventLog(joinpath(vault.outdir, "events.jsonl"))

    # Early skip: load manifest, subtract completed keys
    m = load_manifest(vault)
    todo = todo_keys(m, collect(keys))

    if isempty(todo)
        log_event(log, :skip_complete; stage=stage, total=length(keys))
        return (stage=stage, done=0, err=0, skipped=length(keys), total=length(keys))
    end

    log_event(log, :stage_start; stage=stage, total=length(keys), todo=length(todo))

    n_done = 0
    n_err = 0
    n_busy = 0
    n_gave_up = 0
    for key in todo
        kstr = canonical(key)
        klock = KeyLock(
            _key_lock_dir(vault, key);
            stale_after=opts.stale_after,
            heartbeat_interval=opts.heartbeat_interval,
        )

        outcome = with_key_lock(klock) do
            # Re-check after acquiring the lock — another master may have
            # finished this key between our manifest read and lock acquire.
            if DataVault.is_done(vault, key)
                return :already_done
            end
            DataVault.mark_running!(vault, key)
            return _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts)
        end

        if outcome === nothing
            # Another master is running this key
            log_event(log, :lock_busy; stage=stage, key=kstr)
            n_busy += 1
        elseif outcome === :already_done
            add_complete!(m, key)
        elseif outcome === :ok
            add_complete!(m, key)
            n_done += 1
        elseif outcome === :gave_up
            n_gave_up += 1
            n_err += 1
        else  # :error (single-attempt failure)
            n_err += 1
        end
    end

    # Persist the updated manifest once per stage end
    save_manifest(m)

    log_event(
        log,
        :stage_done;
        stage=stage,
        total=length(keys),
        done=n_done,
        err=n_err,
        busy=n_busy,
        gave_up=n_gave_up,
        skipped=length(keys) - length(todo),
    )
    return (
        stage=stage,
        done=n_done,
        err=n_err,
        busy=n_busy,
        gave_up=n_gave_up,
        skipped=length(keys) - length(todo),
        total=length(keys),
    )
end

"""
    _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts) -> Symbol

Execute `work_fn(key)` up to `opts.max_attempts` times. Returns:
  :ok       — payload saved and mark_done! called
  :gave_up  — all attempts failed, final `:gave_up` event logged
  :error    — single-attempt config (`max_attempts == 1`) that failed once
"""
function _run_one_with_retry!(
    work_fn,
    vault::Vault,
    key::DataKey,
    kstr::String,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
)
    last_err = nothing
    for attempt in 1:opts.max_attempts
        log_event(log, :key_start; stage=stage, key=kstr, attempt=attempt)
        t0 = time()
        try
            payload = work_fn(key)
            payload isa Dict || error(
                "work_fn must return a Dict (got $(typeof(payload))). " *
                "Wrap scalars as e.g. Dict(\"value\" => x).",
            )
            DataVault.save!(vault, key, payload)
            DataVault.mark_done!(vault, key)
            log_event(
                log, :key_done; stage=stage, key=kstr, secs=time() - t0, attempt=attempt
            )
            return :ok
        catch e
            last_err = sprint(showerror, e)
            log_event(log, :error; stage=stage, key=kstr, attempt=attempt, err=last_err)
            if attempt < opts.max_attempts
                log_event(log, :retry; stage=stage, key=kstr, next_attempt=attempt + 1)
                sleep(0.1 * attempt)  # linear backoff
            end
        end
    end
    log_event(
        log, :gave_up; stage=stage, key=kstr, attempts=opts.max_attempts, err=last_err
    )
    return opts.max_attempts == 1 ? :error : :gave_up
end

export RunOpts, run!, manifest_root, load_manifest
