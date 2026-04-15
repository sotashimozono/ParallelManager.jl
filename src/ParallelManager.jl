"""
    ParallelManager

HPC experiment runtime for Julia.

Wraps `ParamIO.jl` and `DataVault.jl` with a unified `run!` that handles
parallel dispatch, advisory locking, `.done` rollups, structured event
logging, and retry. Designed to replace the recurring "glue" layer that
every HPC research project re-invents.

# Modules

Each file is one concern, one module-scope piece. They can be used
independently.

| File            | Public API                                              |
| :-------------- | :------------------------------------------------------ |
| `AtomicIO.jl`   | [`atomic_write`](@ref), [`atomic_touch`](@ref)          |
| `EventLog.jl`   | [`EventLog`](@ref), [`log_event`](@ref)                 |
| `Manifest.jl`   | [`Manifest`](@ref), [`load_manifest`](@ref), [`save_manifest`](@ref), [`add_complete!`](@ref), [`is_complete`](@ref), [`todo_keys`](@ref), [`manifest_path`](@ref) |
| `KeyLock.jl`    | [`KeyLock`](@ref), [`with_key_lock`](@ref), [`try_acquire`](@ref), [`release!`](@ref), [`touch_heartbeat`](@ref), [`is_stale`](@ref), [`reclaim!`](@ref) |
| `InitWorkers.jl`| [`init_workers!`](@ref), [`detect_mode`](@ref)          |
| `Run.jl`        | [`run!`](@ref), [`RunOpts`](@ref), [`manifest_root`](@ref) |

# Quick start

```julia
using ParamIO, DataVault, ParallelManager

spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

ParallelManager.init_workers!(mode=:auto)
work_fn = key -> Dict{String,Any}("x" => compute(key))
ParallelManager.run!(work_fn, vault, keys)
```

# Design constraints

1. `work_fn` is a **pure function** — no IO, no globals, no logging. All of
   that lives in the runtime.
2. There is **no per-item `println` API**. Use [`log_event`](@ref) for
   structured events. Per-item prints are the reason `FiniteTemperature.jl`
   used to generate 300 MB job logs.
3. The `Manifest` is **monotonic** — keys are only added, never removed.
   Re-computing the same root is considered a contract violation; branch
   into a new `outdir` instead.
4. Multi-master coordination uses `mkdir` and `rename` only — no `flock`,
   no central service. Safe on NFS.

# See also

- `ParamIO.canonical` — the stable string form used by [`Manifest`](@ref)
  and [`KeyLock`](@ref) as a directory-safe key identity.
- `DataVault.Vault`, `DataVault.save!`, `DataVault.is_done` — the storage
  layer [`run!`](@ref) delegates to.
"""
module ParallelManager

# Do NOT add a per-item `println` API anywhere in this module. Structured
# events go through EventLog (JSONL) only. This is a structural answer to
# the痛点 of per-item println noise.

include("AtomicIO.jl")
include("EventLog.jl")
include("Manifest.jl")
include("KeyLock.jl")
include("InitWorkers.jl")
include("Run.jl")

end # module ParallelManager
