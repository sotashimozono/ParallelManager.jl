"""
test_preflight.jl — the report machinery, and proof that each check can refuse.

The second half is the load-bearing one. A gate that has only ever returned
"ok" is indistinguishable from a gate that cannot fail, so every check here is
given input it MUST reject, and the rejection is asserted to carry the right
layer — a layering that cannot distinguish two causes is decoration.

`check_injective!` is the one worth this care. In the study this was ported out
of, a content-blind float rendering collapsed `h = 0.002` and `0.004` onto one
directory: 168 sweep points became 132, and because the *status marker*
collided too, the second point of each pair was reported complete without ever
running. No error anywhere.
"""

using Test
using ParallelManager

@testset "Preflight" begin
    @testset "an empty report is launchable and says so" begin
        r = PreflightReport(Finding[])
        @test isempty(r)
        @test launchable(r)
        @test n_errors(r) == 0
        @test n_warns(r) == 0
        @test occursin("nothing to report", sprint(show, r))
    end

    @testset "warnings do not block; errors do" begin
        warn = Finding(:grid, :warn, "cfg", "a note")
        err = Finding(:grid, :error, "cfg", "a blocker")
        @test launchable(PreflightReport([warn]))
        @test !launchable(PreflightReport([warn, err]))
        @test n_warns(PreflightReport([warn, err])) == 1
        @test n_errors(PreflightReport([warn, err])) == 1
    end

    @testset "show groups by layer and states the verdict" begin
        r = PreflightReport([
            Finding(:grid, :error, "a.toml", "off grid"),
            Finding(:injective, :error, "b.toml", "collision"),
        ])
        out = sprint(show, r)
        @test occursin("grid", out)
        @test occursin("injective", out)
        @test occursin("NOT launchable", out)
        @test occursin("errors=2", out)
    end

    # ── on_grid ─────────────────────────────────────────────────────────────

    @testset "on_grid" begin
        @test on_grid(0.4, 0.2)
        @test on_grid(0.0, 0.2)
        @test on_grid(20.0, 0.2)
        @test !on_grid(0.5, 0.2)          # steps 0.4 -> 0.6, never lands
        @test !on_grid(1.5, 0.2)
        # accumulated floats must still count as on-grid
        t = 0.0
        for _ in 1:100
            t += 0.2
        end
        @test on_grid(t, 0.2)
        @test t != 20.0                   # …and it is NOT exactly 20.0
    end

    # ── check_injective! ────────────────────────────────────────────────────

    @testset "check_injective! is silent on distinct paths" begin
        fs = Finding[]
        check_injective!(fs, :injective, "cfg", "obs", ["a", "b", "c"])
        @test isempty(fs)
    end

    @testset "check_injective! reports a collision, with the offender" begin
        fs = Finding[]
        check_injective!(fs, :injective, "cfg", "obs", ["a", "b", "a"])
        @test length(fs) == 1
        f = only(fs)
        @test f.layer === :injective
        @test f.severity === :error
        @test f.where == "cfg"
        @test occursin("3 parameter points → 2 distinct paths", f.message)
        @test occursin("a", f.message)          # names which path collided
        # the consequence, not just the count — this is the half that makes work
        # be SKIPPED rather than merely overwritten
        @test occursin("never ran", f.message)
    end

    @testset "check_injective! counts every duplicate, not just the first" begin
        fs = Finding[]
        check_injective!(fs, :injective, "cfg", "obs", ["a", "a", "b", "b", "c"])
        @test occursin("5 parameter points → 3 distinct paths", only(fs).message)
    end

    @testset "the collision it names is deterministic" begin
        # Set iteration order is not; the message must not vary run to run or a
        # CI failure is unreproducible.
        msgs = map(1:8) do _
            fs = Finding[]
            check_injective!(fs, :injective, "cfg", "obs", ["z", "z", "a", "a"])
            only(fs).message
        end
        @test length(unique(msgs)) == 1
    end

    # ── check_opens! ────────────────────────────────────────────────────────

    @testset "check_opens! passes the value through on success" begin
        fs = Finding[]
        v = check_opens!(fs, :config, "cfg", () -> 42)
        @test v == 42
        @test isempty(fs)
    end

    @testset "check_opens! records a failure instead of throwing" begin
        fs = Finding[]
        v = check_opens!(fs, :config, "cfg", () -> error("log.toml conflict"))
        @test v === nothing
        @test length(fs) == 1
        @test only(fs).layer === :config
        @test occursin("could not open the vault", only(fs).message)
        @test occursin("log.toml conflict", only(fs).message)
    end

    # ── representative_keys ─────────────────────────────────────────────────

    @testset "representative_keys collapses samples, keeps points" begin
        mk(n, s) = ParallelManager.DataKey(Dict{String,Any}("N" => n), s)
        keys = [mk(4, 1), mk(4, 2), mk(4, 3), mk(8, 1), mk(8, 2)]
        reps = representative_keys(keys)
        @test length(reps) == 2
        @test Set(k.params["N"] for k in reps) == Set([4, 8])
        # order is the first-seen order, so a report reads the same twice
        @test [k.params["N"] for k in reps] == [4, 8]
    end

    @testset "why collapsing matters: samples are not collisions" begin
        # Counting over all keys would report n_samples-1 false collisions per
        # point, because a path is a property of the POINT.
        mk(n, s) = ParallelManager.DataKey(Dict{String,Any}("N" => n), s)
        keys = [mk(4, s) for s in 1:5]
        dir_of(k) = "N$(k.params["N"])"          # sample lives in the filename
        fs = Finding[]
        check_injective!(fs, :injective, "cfg", "dir", [dir_of(k) for k in keys])
        @test !isempty(fs)                        # naive: 5 -> 1, looks broken
        fs2 = Finding[]
        check_injective!(
            fs2, :injective, "cfg", "dir", [dir_of(k) for k in representative_keys(keys)]
        )
        @test isempty(fs2)                        # correct: one point, one dir
    end
end
