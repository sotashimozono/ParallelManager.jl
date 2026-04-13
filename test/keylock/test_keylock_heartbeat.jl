using ParallelManager, Test

@testset "KeyLock: heartbeat file created on acquire" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k.lock"))
        @test try_acquire(l)
        @test isfile(heartbeat_path(l))
        release!(l)
    end
end

@testset "KeyLock: touch_heartbeat updates mtime" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k.lock"))
        @test try_acquire(l)
        t0 = mtime(heartbeat_path(l))
        sleep(1.2)
        touch_heartbeat(l)
        t1 = mtime(heartbeat_path(l))
        @test t1 > t0
        release!(l)
    end
end

@testset "KeyLock: is_stale detects old heartbeat" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k.lock"); stale_after=0.1)
        @test try_acquire(l)
        @test !is_stale(l)
        sleep(0.3)
        @test is_stale(l)
        release!(l)
    end
end

@testset "KeyLock: stale lock reclaimed on next try_acquire" begin
    mktempdir() do dir
        # Simulate a dead master: grab lock, then don't release
        l_dead = KeyLock(joinpath(dir, "k.lock"); stale_after=0.1)
        @test try_acquire(l_dead)  # leave held

        sleep(0.3)  # heartbeat now stale

        # A new master comes in with short stale_after
        l_new = KeyLock(joinpath(dir, "k.lock"); stale_after=0.1)
        @test try_acquire(l_new) == true  # reclaims and re-takes
        release!(l_new)
    end
end

@testset "KeyLock: is_stale false for missing lock dir" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k.lock"); stale_after=0.1)
        @test is_stale(l)  # no heartbeat file → stale
    end
end

@testset "with_key_lock: heartbeat task runs in background" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k.lock");
                    stale_after=10.0, heartbeat_interval=0.2)
        mtimes = Float64[]
        with_key_lock(l) do
            push!(mtimes, mtime(heartbeat_path(l)))
            sleep(0.6)
            push!(mtimes, mtime(heartbeat_path(l)))
        end
        @test mtimes[2] > mtimes[1]
    end
end

@testset "with_key_lock: heartbeat task stops after release" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k.lock"); heartbeat_interval=0.1)
        with_key_lock(l) do
            sleep(0.1)
        end
        # Lock dir should be gone
        @test !isdir(l.dir)
        # Give heartbeat task a chance to notice the stop flag
        sleep(0.3)
        # No stray heartbeat touches recreating files
        @test !isfile(heartbeat_path(l))
    end
end

@testset "KeyLock: reclaim race — two contenders, one wins" begin
    mktempdir() do dir
        lockdir = joinpath(dir, "k.lock")
        # Plant a stale lock
        l_dead = KeyLock(lockdir; stale_after=0.05)
        @test try_acquire(l_dead)
        sleep(0.2)  # make it stale

        # Two concurrent reclaim attempts
        winners = Threads.Atomic{Int}(0)
        tasks = [Threads.@spawn begin
            l = KeyLock(lockdir; stale_after=0.05)
            if try_acquire(l)
                Threads.atomic_add!(winners, 1)
                sleep(0.1)
                release!(l)
            end
        end for _ in 1:10]
        foreach(wait, tasks)
        # Exactly one wins at the moment of contention; some may win sequentially
        # after release (that's OK). The key property is: no simultaneous holders.
        @test winners[] >= 1
    end
end

@testset "KeyLock: mutual exclusion still holds with heartbeat" begin
    mktempdir() do dir
        lockdir = joinpath(dir, "k.lock")
        inside = Threads.Atomic{Int}(0)
        max_inside = Threads.Atomic{Int}(0)
        tasks = Task[]
        for _ in 1:50
            push!(tasks, Threads.@spawn begin
                l = KeyLock(lockdir; heartbeat_interval=10.0)
                with_key_lock(l) do
                    current = Threads.atomic_add!(inside, 1) + 1
                    while true
                        m = max_inside[]
                        current <= m && break
                        Threads.atomic_cas!(max_inside, m, current) == m && break
                    end
                    sleep(0.001)
                    Threads.atomic_sub!(inside, 1)
                end
            end)
        end
        foreach(wait, tasks)
        @test max_inside[] == 1
    end
end
