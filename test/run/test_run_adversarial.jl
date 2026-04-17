# ─────────────────────────────────────────────────────────────────────────────
# Adversarial tests for run! / run_loop! / KeyLock / Manifest.
#
# These probe failure modes the happy-path tests miss:
#  - stop_flag written DURING pmap fan-out (not only before run!)
#  - KeyLock stale reclaim when holder dies (simulated by stale mtime)
#  - Corrupt manifest.jld2 → empty manifest, no crash
#  - work_fn that returns non-Dict / throws after partial completion
#  - run! over a vault where some keys already have `.running` orphans
#    (exactly the situation the try-finally fix in #8 created a regression for)
# ─────────────────────────────────────────────────────────────────────────────

using ParallelManager, Test, DataVault, ParamIO, JSON3, JLD2
using Distributed

const FIXTURE_CFG_A = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_a(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_A; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allk_a(v) = ParamIO.expand(v.spec)

# ═════════════════════════════════════════════════════════════════════════════
# 1. stop_flag written mid-run by a concurrent task
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: stop_flag raised mid-execution by concurrent task" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        @test length(keys) >= 4

        stop = joinpath(outdir, "STOP_NOW")
        # Delay the stop flag by 50 ms so a few keys finish first
        t = Timer(_ -> touch(stop), 0.05)

        # work_fn sleeps so we're sure the stop is seen after some work
        counter = Ref(0)
        work_fn = k -> begin
            counter[] += 1
            sleep(0.02)
            Dict{String,Any}("x" => 1)
        end

        result = run!(work_fn, v, keys; opts=RunOpts(stop_flag=stop))
        close(t)

        # At least one key ran before stop kicked in, at least one was skipped
        @test result.done >= 1
        @test result.done < length(keys)  # stop really cut us short
        # Remaining keys are either counted as :stop (if we reached their
        # dispatch) or just dropped from the todo list (sequential loop
        # break). Either way, fewer done+stop than length means some were
        # silently un-processed.
        @test counter[] == result.done  # work_fn only called for done keys
        isfile(stop) && rm(stop; force=true)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 2. .running orphans from crashed prior runs are resumed cleanly
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: stale .running orphan is overwritten and completed" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        target = keys[1]

        # Simulate a crashed prior run: .running exists but no .done
        DataVault.mark_running!(v, target)
        @test DataVault.is_running(v, target)
        @test !DataVault.is_done(v, target)

        # New run should still process this key (try-finally guards ensure
        # clear_running! is always called)
        work_fn = k -> Dict{String,Any}("x" => 1)
        result = run!(work_fn, v, keys)
        @test result.done == length(keys)
        @test DataVault.is_done(v, target)
        # .running must be cleaned up
        @test !DataVault.is_running(v, target)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 3. Corrupt manifest.jld2 → treated as empty, run completes normally
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: corrupt manifest.jld2 is recoverable" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        # Write a garbage file where the manifest would be
        mpath = manifest_path(manifest_root(v), Symbol(v.run))
        mkpath(dirname(mpath))
        write(mpath, "this is not a JLD2 file")
        @test isfile(mpath)

        # run! should not crash; load_manifest returns empty and runs all keys
        work_fn = k -> Dict{String,Any}("x" => 1)
        result = run!(work_fn, v, keys)
        @test result.done == length(keys)
        # After run, the corrupt file is overwritten with a valid manifest
        m = load_manifest(v)
        @test length(m.complete) == length(keys)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 4. KeyLock: stale reclaim when holder died before first heartbeat
# ═════════════════════════════════════════════════════════════════════════════

@testset "KeyLock: reclaim of a stale lock without heartbeat file" begin
    mktempdir() do dir
        # Simulate a dead holder: acquired the lock (mkdir) but died before
        # the first heartbeat touch → no heartbeat file inside.
        lock_dir = joinpath(dir, "k")
        l = KeyLock(lock_dir; stale_after=0.01, heartbeat_interval=0.005)

        # Manually create lock dir without heartbeat (dead holder state)
        mkdir(lock_dir)
        write(joinpath(lock_dir, "holder"), "ghost:9999")
        # No heartbeat file → is_stale should fall back to dir mtime

        sleep(0.05)  # Wait beyond stale_after
        @test is_stale(l) == true

        # Another contender should be able to reclaim + acquire
        @test try_acquire(l) == true
        @test isdir(lock_dir)
        # heartbeat file should now exist (fresh acquisition)
        @test isfile(joinpath(lock_dir, "heartbeat"))
        release!(l)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 5. Multi-task race with VERY short heartbeat interval
#    (stress test: ensures heartbeat loop doesn't deadlock with release)
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: 8-master race with fast heartbeat (stress)" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        invoked = Dict{String,Threads.Atomic{Int}}()
        for k in keys
            invoked[canonical(k)] = Threads.Atomic{Int}(0)
        end
        work_fn = k -> begin
            Threads.atomic_add!(invoked[canonical(k)], 1)
            sleep(0.01)
            Dict{String,Any}("x" => 1)
        end

        # 8 concurrent threads, very aggressive heartbeat
        tasks = [
            Threads.@spawn(run!(work_fn, v, keys;
                                opts=RunOpts(heartbeat_interval=0.05, stale_after=1.0)))
            for _ in 1:8
        ]
        foreach(wait, tasks)

        for k in keys
            @test DataVault.is_done(v, k)
            # Under contention, each key should be invoked at most a few times
            # (ideally exactly once, but re-runs inside stale reclaim windows
            # are acceptable)
            @test invoked[canonical(k)][] >= 1
            @test invoked[canonical(k)][] <= 3
        end
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 6. merge_and_save_manifest!: two concurrent masters don't lose keys
# ═════════════════════════════════════════════════════════════════════════════

@testset "merge_and_save_manifest!: preserves keys across concurrent writers" begin
    with_vault_a() do v, outdir
        # Pre-create two disjoint manifests on different in-memory objects
        # but same on-disk location, simulate race where each master
        # starts with its own completed set.
        ka = ParamIO.DataKey(Dict{String,Any}("N" => 4, "J" => 0.5), 0)
        kb = ParamIO.DataKey(Dict{String,Any}("N" => 4, "J" => 1.0), 0)
        kc = ParamIO.DataKey(Dict{String,Any}("N" => 8, "J" => 0.5), 0)

        root = manifest_root(v)
        stage = Symbol(v.run)

        m_A = Manifest(stage, root)
        add_complete!(m_A, ka)
        add_complete!(m_A, kb)
        # Save A's state first (plain save, like an older master might have done)
        save_manifest(m_A)

        # Master B has only {c} in memory; merges should give {a,b,c}
        m_B = Manifest(stage, root)
        add_complete!(m_B, kc)
        merge_and_save_manifest!(m_B)

        final = load_manifest(root, stage)
        @test is_complete(final, ka)
        @test is_complete(final, kb)
        @test is_complete(final, kc)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 7. run! doesn't crash when work_fn throws on EVERY key
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: all keys fail → no crash, all .running cleaned" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        result = run!(
            k -> error("always fails"),
            v, keys;
            opts=RunOpts(max_attempts=2)
        )
        @test result.done == 0
        @test result.gave_up == length(keys)
        # No .running orphans
        for k in keys
            @test !DataVault.is_running(v, k)
            @test !DataVault.is_done(v, k)
        end
    end
end
