using ParallelManager, Test, ParamIO

mkkey(params; sample=0) = DataKey(Dict{String,Any}(params...), sample)

@testset "Manifest: empty when absent" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        @test m.stage == :phase1
        @test isempty(m.complete)
    end
end

@testset "Manifest: add, save, load round-trip" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        k1 = mkkey(["N" => 8, "J" => 1.0]; sample=1)
        k2 = mkkey(["N" => 8, "J" => 2.0]; sample=1)
        add_complete!(m, k1)
        add_complete!(m, k2)
        save_manifest(m)

        @test isfile(manifest_path(dir, :phase1))

        m2 = load_manifest(dir, :phase1)
        @test is_complete(m2, k1)
        @test is_complete(m2, k2)
        @test length(m2.complete) == 2
    end
end

@testset "Manifest: todo_keys subtracts complete set" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        k1 = mkkey(["N" => 8]; sample=1)
        k2 = mkkey(["N" => 16]; sample=1)
        k3 = mkkey(["N" => 32]; sample=1)
        add_complete!(m, k1)
        add_complete!(m, k2)
        todo = todo_keys(m, [k1, k2, k3])
        @test length(todo) == 1
        @test todo[1] == k3
    end
end

@testset "Manifest: all done -> todo_keys empty" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        keys = [mkkey(["N" => n]; sample=1) for n in (4, 8, 16)]
        for k in keys
            add_complete!(m, k)
        end
        @test isempty(todo_keys(m, keys))
    end
end

@testset "Manifest: corrupted file treated as empty" begin
    mktempdir() do dir
        p = manifest_path(dir, :phase1)
        mkpath(dirname(p))
        write(p, "not a jld2 file")
        m = load_manifest(dir, :phase1)
        @test isempty(m.complete)
    end
end

@testset "Manifest: monotonic (no remove API)" begin
    # Structural check: there is no `remove_complete!` exported
    names_exported = names(ParallelManager)
    @test !(:remove_complete! in names_exported)
    @test !(:delete_complete! in names_exported)
end

@testset "Manifest: bench 3600 keys load+todo < 500ms" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        keys = [mkkey(["N" => i]; sample=1) for i in 1:3600]
        for k in keys
            add_complete!(m, k)
        end
        save_manifest(m)

        # Measure second-time load + todo_keys
        t0 = time()
        m2 = load_manifest(dir, :phase1)
        td = todo_keys(m2, keys)
        elapsed = time() - t0
        @test isempty(td)
        @test elapsed < 0.5  # 500ms loose budget; full target is <100ms (todo 04)
        @info "Manifest bench" n_keys=3600 elapsed_s=elapsed
    end
end

@testset "Manifest: field order independence via canonical" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        k_a = DataKey(Dict{String,Any}("N" => 8, "J" => 1.0), 1)
        k_b = DataKey(Dict{String,Any}("J" => 1.0, "N" => 8), 1)
        add_complete!(m, k_a)
        @test is_complete(m, k_b)
    end
end
