# InitWorkers — unified worker bootstrap for Threads / Distributed / SLURM
#
# Absorbs the existing `HybridInit.init_hybrid_scm!` from
# `Vault/.vault/templates/templateHPC.jl/src/parallel/init.jl` so that
# templateHPC can be reduced to a thin wrapper.

using Distributed
using LinearAlgebra
using SlurmClusterManager

"""
    init_workers!(; mode=:auto, master_blas=1, verbose=true) -> Symbol

Bootstrap worker processes / threads according to `mode`. Returns the mode
that was actually used.

- `:auto`       — pick `:slurm` if `SLURM_JOB_ID` is set, else `:threads` if
                  `Threads.nthreads() > 1`, else `:sequential`
- `:slurm`      — `addprocs(SlurmClusterManager.SlurmManager())` with BLAS
                  tuning. Absorbs the existing `init_hybrid_scm!` pattern.
- `:distributed`— `addprocs(JULIA_SLURM_N_WORKERS or 1)` single-node
- `:threads`    — no-op; caller uses `Threads.@threads`
- `:sequential` — no-op; caller iterates serially (debug / tests)

Idempotent: calling multiple times with worker processes already present
does not double-add. BLAS thread settings are always (re)applied.
"""
function init_workers!(; mode::Symbol=:auto, master_blas::Int=1, verbose::Bool=true)
    actual = mode == :auto ? detect_mode() : mode

    if actual == :sequential
        BLAS.set_num_threads(master_blas)
        verbose && _log_init("sequential", 0, master_blas, master_blas)
        return :sequential

    elseif actual == :threads
        BLAS.set_num_threads(master_blas)
        verbose && _log_init("threads", Threads.nthreads() - 1, master_blas, master_blas)
        return :threads

    elseif actual == :distributed
        n_workers = parse(
            Int, get(ENV, "JULIA_SLURM_N_WORKERS", get(ENV, "SLURM_NTASKS", "1"))
        )
        worker_blas = parse(
            Int, get(ENV, "JULIA_WORKER_CPUS", get(ENV, "SLURM_CPUS_PER_TASK", "1"))
        )
        if n_workers > 0 && nprocs() == 1
            project = dirname(Base.active_project())
            addprocs(n_workers; exeflags="--project=$project")
        end
        _apply_blas(master_blas, worker_blas)
        verbose && _log_init("distributed", n_workers, master_blas, worker_blas)
        return :distributed

    elseif actual == :slurm
        n_workers = parse(
            Int, get(ENV, "JULIA_SLURM_N_WORKERS", get(ENV, "SLURM_NTASKS", "0"))
        )
        worker_blas = parse(
            Int, get(ENV, "JULIA_WORKER_CPUS", get(ENV, "SLURM_CPUS_PER_TASK", "1"))
        )
        if n_workers > 0 && nprocs() == 1
            project = dirname(Base.active_project())
            mgr = withenv("SLURM_NTASKS" => string(n_workers)) do
                SlurmClusterManager.SlurmManager()
            end
            addprocs(mgr; exeflags="--project=$project")
        end
        _apply_blas(master_blas, worker_blas)
        verbose && _log_init("slurm", n_workers, master_blas, worker_blas)
        return :slurm

    else
        error("init_workers!: unknown mode $(actual)")
    end
end

"""
    detect_mode() -> Symbol

Inspect the environment to choose a default mode.
"""
function detect_mode()::Symbol
    haskey(ENV, "SLURM_JOB_ID") && return :slurm
    Threads.nthreads() > 1 && return :threads
    return :sequential
end

function _apply_blas(master_blas::Int, worker_blas::Int)
    if nprocs() > 1
        _w = worker_blas
        @everywhere workers() begin
            Core.eval(Main, :(using LinearAlgebra))
        end
        @everywhere workers() BLAS.set_num_threads($_w)
    end
    BLAS.set_num_threads(master_blas)
    return nothing
end

function _log_init(mode::String, n_workers::Int, master_blas::Int, worker_blas::Int)
    # Note: this is init-time informational output, not per-item. OK to use
    # println here (single line, once per run).
    println("=== ParallelManager.init_workers! ($mode) ===")
    println("  workers     : $n_workers")
    println("  master BLAS : $master_blas")
    println("  worker BLAS : $worker_blas")
    println("  total procs : $(nprocs())")
    println("=============================================")
    return nothing
end

export init_workers!, detect_mode
