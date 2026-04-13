# Manifest — rollup index of completed keys per stage (痛点 #6)
#
# FiniteTemperature.jl stat'd 3600 `.done` files per job (10 min). A Manifest
# collapses that to a single JLD2 read — O(1) on full-done re-runs.

using JLD2
using ParamIO: DataKey, canonical

"""
    Manifest(stage, root, complete)

Rollup index for a single stage under a vault root. `complete` holds the
canonical string of every key known to be finished. Monotonic: once a key is
added it is not removed (contract C5 in the ParallelManager spec).

The on-disk form is `<root>/<stage>/manifest.jld2` with a single field
`complete::Vector{String}`. Vector (not Set) is used for JLD2 portability;
`load_manifest` rehydrates to a Set for O(1) lookup.
"""
mutable struct Manifest
    stage::Symbol
    root::String
    complete::Set{String}
end

Manifest(stage::Symbol, root::AbstractString) =
    Manifest(stage, String(root), Set{String}())

"""
    manifest_path(root, stage)

Return `<root>/<stage>/manifest.jld2`.
"""
manifest_path(root::AbstractString, stage::Symbol) =
    joinpath(String(root), String(stage), "manifest.jld2")

"""
    load_manifest(root, stage) -> Manifest

Read `manifest.jld2` if present; otherwise return an empty Manifest.
A corrupted manifest (read fails) is treated as empty — the worst case is
re-running some keys, which is safe.
"""
function load_manifest(root::AbstractString, stage::Symbol)::Manifest
    p = manifest_path(root, stage)
    if !isfile(p)
        return Manifest(stage, root)
    end
    try
        data = JLD2.load(p)
        items = get(data, "complete", String[])
        return Manifest(stage, String(root), Set{String}(items))
    catch
        return Manifest(stage, String(root))
    end
end

"""
    save_manifest(m)

Atomically write `m.complete` to disk. Uses `atomic_write` with a temp file
+ rename so a crashed master can't leave a half-written manifest.
"""
function save_manifest(m::Manifest)
    p = manifest_path(m.root, m.stage)
    items = sort!(collect(m.complete))
    mkpath(dirname(p))
    # JLD2 writes to a path, so we do the tmp + rename dance directly.
    tmp = string(p, ".tmp.", getpid(), ".", rand(UInt32))
    try
        JLD2.jldsave(tmp; complete=items)
        mv(tmp, p; force=true)
    catch
        isfile(tmp) && try
            rm(tmp; force=true)
        catch
        end
        rethrow()
    end
    return p
end

"""
    add_complete!(m, key)

Add `canonical(key)` to the in-memory complete set. Not persisted until
`save_manifest` is called. Monotonic: no `remove_complete!`.
"""
add_complete!(m::Manifest, key::DataKey) = push!(m.complete, canonical(key))

"""
    is_complete(m, key) -> Bool

True iff `key` is already in the manifest.
"""
is_complete(m::Manifest, key::DataKey) = canonical(key) in m.complete

"""
    todo_keys(m, keys) -> Vector{DataKey}

Return the subset of `keys` that are not yet complete — the work list for
the next `run!`. Full-done case returns an empty vector (for early skip).
"""
todo_keys(m::Manifest, keys::AbstractVector{DataKey}) =
    [k for k in keys if !is_complete(m, k)]

export Manifest, manifest_path, load_manifest, save_manifest
export add_complete!, is_complete, todo_keys
