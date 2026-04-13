using ParallelManager, Test

@testset "KeyLock: first acquire succeeds" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k1.lock"))
        @test try_acquire(l) == true
        @test isdir(l.dir)
        @test isfile(holder_path(l))
        holder = read(holder_path(l), String)
        @test occursin(":", holder)  # "host:pid"
    end
end

@testset "KeyLock: second acquire on same dir fails" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k1.lock"))
        @test try_acquire(l) == true
        l2 = KeyLock(joinpath(dir, "k1.lock"))
        @test try_acquire(l2) == false
    end
end

@testset "KeyLock: release then re-acquire" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k1.lock"))
        @test try_acquire(l) == true
        release!(l)
        @test !isdir(l.dir)
        @test try_acquire(l) == true
    end
end

@testset "KeyLock: release is idempotent" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k1.lock"))
        release!(l)  # no-op when not held
        @test !isdir(l.dir)
    end
end

@testset "with_key_lock: returns f() when acquired" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k1.lock"))
        result = with_key_lock(l) do
            return 42
        end
        @test result == 42
        @test !isdir(l.dir)  # released after block
    end
end

@testset "with_key_lock: returns nothing when busy" begin
    mktempdir() do dir
        l1 = KeyLock(joinpath(dir, "k1.lock"))
        @test try_acquire(l1) == true  # permanently held for this test
        l2 = KeyLock(joinpath(dir, "k1.lock"))
        result = with_key_lock(l2) do
            return 42
        end
        @test result === nothing
        release!(l1)
    end
end

@testset "with_key_lock: releases on exception" begin
    mktempdir() do dir
        l = KeyLock(joinpath(dir, "k1.lock"))
        @test_throws ErrorException with_key_lock(l) do
            error("boom")
        end
        @test !isdir(l.dir)
    end
end

@testset "KeyLock: mutual exclusion under contention" begin
    # The right invariant: at any single instant, at most one task is inside
    # the critical section. Sequential release/acquire is allowed.
    mktempdir() do dir
        lockdir = joinpath(dir, "race.lock")
        inside = Threads.Atomic{Int}(0)
        max_inside = Threads.Atomic{Int}(0)
        tasks = Task[]
        for _ in 1:100
            push!(tasks, Threads.@spawn begin
                l = KeyLock(lockdir)
                with_key_lock(l) do
                    current = Threads.atomic_add!(inside, 1) + 1
                    while true
                        m = max_inside[]
                        current <= m && break
                        Threads.atomic_cas!(max_inside, m, current) == m && break
                    end
                    sleep(0.001)  # hold long enough to expose races
                    Threads.atomic_sub!(inside, 1)
                end
            end)
        end
        foreach(wait, tasks)
        @test max_inside[] == 1
    end
end
