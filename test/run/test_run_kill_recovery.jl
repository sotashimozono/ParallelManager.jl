# test_run_kill_recovery.jl — paths stay clean after a killed job
#
# These tests exercise the recovery path that kicks in on the NEXT
# `run!` invocation after a previous master was SIGKILL'd or
# SIGTERM'd mid-execution.  The on-disk artefacts left behind by a
# crashed master are:
#
#   status/<...>/sample_NNN.running   (DataVault)
#   locks/<...>/sample_NNN/holder     (ParallelManager)
#   locks/<...>/sample_NNN/heartbeat  (ParallelManager)
#
# The desired behaviour is:
#
# 1. A new `run!` sees these leftovers, reclaims the (stale) lock, and
#    re-executes the key normally, producing `.done` and purging the
#    `.running` marker along the way.
# 2. After `DataVault.cleanup_stale(vault)` runs (directly or via
#    `run_managed!` in downstream wrappers), the paths are the SAME as
#    they would have been on a clean re-run — no residual artefacts.
# 3. The lock directory layout matches DataVault's `status/` tree
#    (`<param_path>/sample_<NNN>/...`) so the whole per-key footprint
#    fits in a few dozen characters instead of embedding the full
#    `canonical(key)`.

using ParallelManager, Test, DataVault, ParamIO, Dates

const FIXTURE_CFG_K = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_k(f; run::AbstractString="kill_recovery")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_K; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allk_k(v) = ParamIO.expand(v.spec)

"Fabricate a stale `.running` file whose heartbeat is `age_secs` old."
function _fabricate_stale_running!(
    vault::DataVault.Vault, key::DataVault.DataKey; age_secs::Real=3600
)
    DataVault.mark_running!(vault, key)
    path = ParallelManager.DataVault._running_file(vault, key)
    old_dt = Dates.now() - Dates.Second(round(Int, age_secs))
    open(path, "w") do io
        println(io, "pid=", getpid())
        println(io, "started=", Dates.format(old_dt, "yyyy-mm-ddTHH:MM:SS"))
        println(io, "heartbeat=", Dates.format(old_dt, "yyyy-mm-ddTHH:MM:SS"))
    end
    return path
end

"""
    _fabricate_stale_lock!(vault, key; age_secs=3600)

Fabricate a lock dir whose `heartbeat` file's mtime is `age_secs`
older than `time()`.  `KeyLock.is_stale` compares `time() -
mtime(heartbeat)` against `stale_after`, so we use a shell `touch -d`
to backdate the mtime — this is Linux-specific but matches the target
platform (ISSP System B / Ubuntu dev boxes) and avoids the
`sleep(stale_after)` anti-pattern in tests.
"""
function _fabricate_stale_lock!(
    vault::DataVault.Vault, key::DataVault.DataKey; age_secs::Real=3600
)
    lockdir = ParallelManager._key_lock_dir(vault, key)
    mkpath(lockdir)
    holder = joinpath(lockdir, "holder")
    hb = joinpath(lockdir, "heartbeat")
    open(holder, "w") do io
        println(io, "pid=999999 host=ghost")
    end
    touch(hb)
    # Backdate heartbeat mtime via `touch -d @<epoch_secs>` so the lock
    # is unambiguously "stale" on any sensible `stale_after` threshold.
    t_epoch = floor(Int, time() - age_secs)
    run(`touch -d @$(t_epoch) $hb`)
    run(`touch -d @$(t_epoch) $holder`)
    return lockdir
end

# ── 1. Lock path must use the short DataVault-style layout ────────────
@testset "lock dir uses <param_path>/sample_<NNN> (not canonical(key))" begin
    with_vault_k() do v, outdir
        key = first(allk_k(v))
        dir = ParallelManager._key_lock_dir(v, key)

        # The short layout has exactly two path components after
        # `locks/<project>/<run>/`: the compact `<param_path>` and a
        # `sample_<NNN>` subdir.  Crucially it must NOT embed the
        # `canonical(key)` "k=v;k=v;..." string.
        rel = replace(dir, outdir * "/" => "")
        parts = split(rel, '/')
        @test parts[1] == "locks"
        @test parts[2] == "pm_test"            # project
        @test parts[3] == "kill_recovery"      # run
        @test !occursin(';', rel)              # no canonical(key) separator
        @test !occursin('=', rel)              # no canonical(key) k=v form
        @test occursin("sample_", parts[end])  # trailing sample_NNN
        @test length(rel) < 120                # drastically shorter than the
        # ~270-char canonical form
    end
end

# ── 2. A stale `.running` left by a previous kill must not prevent
#       re-execution and must be cleaned up after a successful run ─────
@testset "stale .running from a killed master is reclaimable on re-run" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        target = first(keys)

        # Simulate a previous master that marked this key running and
        # then got SIGKILL'd with a long-dead heartbeat.
        stale_running = _fabricate_stale_running!(v, target; age_secs=3600)
        @test isfile(stale_running)
        @test !DataVault.is_done(v, target)

        # cleanup_stale should reclaim it (age > stale_after threshold).
        n = DataVault.cleanup_stale(v; stale_after=600)
        @test n >= 1
        @test !isfile(stale_running)

        # And a fresh run! should now execute the key normally.
        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        result = run!(work_fn, v, keys)
        @test result.done == length(keys)
        for k in keys
            @test DataVault.is_done(v, k)
        end
        # No `.running` leftovers after a clean run.
        for k in keys
            @test !isfile(ParallelManager.DataVault._running_file(v, k))
        end
    end
end

# ── 3. A stale lock directory must not block a subsequent master ──────
@testset "stale lock dir is reclaimed by KeyLock.is_stale on re-acquire" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        target = first(keys)

        # Simulate a previous master that died holding the lock.
        stale_lock = _fabricate_stale_lock!(v, target; age_secs=3600)
        @test isdir(stale_lock)

        # `run!` with a generous `stale_after` is expected to reclaim
        # the ghost lock and proceed to execute the key.
        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        opts = ParallelManager.RunOpts(; stale_after=60.0, heartbeat_interval=5.0)
        result = run!(work_fn, v, keys; opts=opts)

        @test result.done == length(keys)
        @test DataVault.is_done(v, target)
        # After a clean run, `with_key_lock` releases the lock, which
        # leaves the lock dir itself on disk (empty or absent of holder
        # files).  What matters for "no leftover" correctness is that
        # the `.running` marker is gone and `.done` is present.
        @test !isfile(ParallelManager.DataVault._running_file(v, target))
    end
end

# ── 4. End-to-end: stale .running AND stale lock on every key ─────────
@testset "bulk stale cleanup: every key has stale artefacts, all recover" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)

        # Make every key look like a killed master's ghost.
        for k in keys
            _fabricate_stale_running!(v, k; age_secs=7200)
            _fabricate_stale_lock!(v, k; age_secs=7200)
        end

        # Cleanup + normal re-run.
        DataVault.cleanup_stale(v; stale_after=600)

        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        opts = ParallelManager.RunOpts(; stale_after=60.0, heartbeat_interval=5.0)
        result = run!(work_fn, v, keys; opts=opts)

        @test result.done == length(keys)
        for k in keys
            @test DataVault.is_done(v, k)
            @test !isfile(ParallelManager.DataVault._running_file(v, k))
        end
    end
end
