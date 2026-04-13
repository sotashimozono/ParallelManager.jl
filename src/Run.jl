# Run — the facade that ties work_fn to Vault, Lock, Manifest, Log
#
# Evolution across todos:
#   09: minimal (sequential, no manifest, no lock)
#   10: + Manifest (early skip)
#   11: + KeyLock (multi-master)
#   12: + retry

using DataVault
using ParamIO: DataKey, canonical

"""
    RunOpts(; workers=:auto, max_attempts=1, stale_after=600.0,
             heartbeat_interval=60.0)

Execution options for `run!`.
"""
struct RunOpts
    workers::Symbol
    max_attempts::Int
    stale_after::Float64
    heartbeat_interval::Float64
end

function RunOpts(; workers::Symbol=:auto, max_attempts::Int=3,
                 stale_after::Real=600.0, heartbeat_interval::Real=60.0)
    RunOpts(workers, max_attempts, Float64(stale_after), Float64(heartbeat_interval))
end

"""
    _key_lock_dir(vault, key) -> String

Return the per-(vault.run, key) lock directory path.
Lives under the vault outdir, keyed by canonical(key), so multiple masters
on the same vault agree on the location without touching DataVault internals.
"""
function _key_lock_dir(vault::Vault, key::DataKey)
    joinpath(vault.outdir, "locks", vault.spec.study.project_name,
             vault.run, canonical(key))
end

"""
    manifest_root(vault) -> String

Where the ParallelManager manifest file lives for this vault.
One manifest per (project, run).
"""
manifest_root(vault::Vault) =
    joinpath(vault.outdir, "manifest", vault.spec.study.project_name)

"""
    load_manifest(vault) -> Manifest

Convenience overload that reads the manifest for `vault.run` under this vault.
"""
load_manifest(vault::Vault) = load_manifest(manifest_root(vault), Symbol(vault.run))

"""
    run!(work_fn, vault, keys; opts=RunOpts()) -> NamedTuple

Run `work_fn(key) -> Dict` for every `key` in `keys`, persisting through
`vault`. Writes a structured JSONL event log at
`joinpath(vault.outdir, "events.jsonl")`.

Early skip (todo 10): on startup a stage-level Manifest is loaded. Keys
already in the manifest are skipped — when all keys are done, the second
run-through takes O(1) filesystem operations regardless of `length(keys)`.
This is the answer to 痛点 #6 (3600-file rescan every job).

Contract:
- `work_fn` is expected to be a pure function: given a `DataKey`, return a
  `Dict` payload to persist via `DataVault.save!`.
- Exceptions in `work_fn` are caught and logged; the corresponding key's
  `.done` file is not written, so re-runs will pick it up.
- The stage label used for logging is `Symbol(vault.run)`.
- Manifest is monotonic: saved at end-of-stage with every newly completed key.

Sequential execution; multi-master locking is added in todo 11.
"""
function run!(work_fn::Function, vault::Vault, keys::AbstractVector{DataKey};
              opts::RunOpts=RunOpts())
    stage = Symbol(vault.run)
    log = EventLog(joinpath(vault.outdir, "events.jsonl"))

    # Early skip: load manifest, subtract completed keys
    m = load_manifest(vault)
    todo = todo_keys(m, collect(keys))

    if isempty(todo)
        log_event(log, :skip_complete; stage=stage, total=length(keys))
        return (stage=stage, done=0, err=0, skipped=length(keys), total=length(keys))
    end

    log_event(log, :stage_start; stage=stage,
              total=length(keys), todo=length(todo))

    n_done = 0
    n_err = 0
    n_busy = 0
    n_gave_up = 0
    for key in todo
        kstr = canonical(key)
        klock = KeyLock(_key_lock_dir(vault, key);
                        stale_after=opts.stale_after,
                        heartbeat_interval=opts.heartbeat_interval)

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

    log_event(log, :stage_done; stage=stage, total=length(keys),
              done=n_done, err=n_err, busy=n_busy, gave_up=n_gave_up,
              skipped=length(keys) - length(todo))
    return (stage=stage, done=n_done, err=n_err, busy=n_busy,
            gave_up=n_gave_up, skipped=length(keys) - length(todo),
            total=length(keys))
end

"""
    _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts) -> Symbol

Execute `work_fn(key)` up to `opts.max_attempts` times. Returns:
  :ok       — payload saved and mark_done! called
  :gave_up  — all attempts failed, final `:gave_up` event logged
  :error    — single-attempt config (`max_attempts == 1`) that failed once
"""
function _run_one_with_retry!(work_fn, vault::Vault, key::DataKey, kstr::String,
                              stage::Symbol, log::EventLog, opts::RunOpts)
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
            log_event(log, :key_done; stage=stage, key=kstr,
                      secs=time() - t0, attempt=attempt)
            return :ok
        catch e
            last_err = sprint(showerror, e)
            log_event(log, :error; stage=stage, key=kstr,
                      attempt=attempt, err=last_err)
            if attempt < opts.max_attempts
                log_event(log, :retry; stage=stage, key=kstr,
                          next_attempt=attempt + 1)
                sleep(0.1 * attempt)  # linear backoff
            end
        end
    end
    log_event(log, :gave_up; stage=stage, key=kstr,
              attempts=opts.max_attempts, err=last_err)
    return opts.max_attempts == 1 ? :error : :gave_up
end

export RunOpts, run!, manifest_root, load_manifest
