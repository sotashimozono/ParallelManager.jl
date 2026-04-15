# AtomicIO — NFS-safe atomic write helpers.
#
# Solves the class of failures where a master crash between `open("w")` and
# `close` leaves a half-written file that later readers silently consume as
# "complete".

"""
    atomic_write(write_fn, path)

Write `path` atomically: the target either does not exist or contains the
complete output — never partially-written content.

Internally:

1. Writes to `"\$path.tmp.\$(getpid()).\$(rand(UInt32))"` via `write_fn(io)`.
2. Flushes + `fsync`s the tmp file (best-effort on filesystems that don't
   support `fsync` via `ccall`).
3. `mv(tmp, path; force=true)` — POSIX `rename(2)` is atomic, including on
   NFS, so concurrent readers see either the old content or the new.

The parent directory of `path` is created if missing. If `write_fn` throws
an exception, the tmp file is cleaned up and the target `path` is left
untouched.

# Returns

The `path` argument unchanged, so the function is chainable:

```julia
p = atomic_write("out/data.jld2") do io
    write(io, payload_bytes)
end
# p === "out/data.jld2"
```

# Notes

- The tmp filename includes `getpid()` and a random `UInt32` so two
  processes racing on the same `path` do not clobber each other's tmp.
- This helper is file-format agnostic: callers pass any `IO`-consuming
  closure. For JLD2 payloads, [`Manifest`](@ref) uses a similar
  path-level dance because `JLD2.jldsave` needs a path, not an `IO`.
- On a filesystem where `fsync` is not available, the `ccall` is caught
  and ignored; `rename` atomicity still holds.

See also: [`atomic_touch`](@ref) for empty-file markers.
"""
function atomic_write(write_fn::Function, path::AbstractString)
    mkpath(dirname(path))
    tmp = string(path, ".tmp.", getpid(), ".", rand(UInt32))
    try
        open(tmp, "w") do io
            write_fn(io)
            flush(io)
            try
                ccall(:fsync, Cint, (Cint,), fd(io))
            catch
                # fsync not available on this FS; rename is still atomic.
            end
        end
        mv(tmp, path; force=true)
    catch
        # Clean up tmp on failure; do NOT touch the target path.
        isfile(tmp) && try
            rm(tmp; force=true)
        catch
        end
        rethrow()
    end
    return path
end

"""
    atomic_touch(path)

Create an empty file at `path` atomically. Convenience wrapper over
[`atomic_write`](@ref) for sentinel files like `.done` markers.

```julia
atomic_touch(joinpath(vault.outdir, "stage1", "done.marker"))
```

Semantics are identical to `atomic_write(_ -> nothing, path)` — the parent
directory is created, the file appears atomically, and no tmp residue is
left behind.
"""
atomic_touch(path::AbstractString) = atomic_write(_ -> nothing, path)

export atomic_write, atomic_touch
