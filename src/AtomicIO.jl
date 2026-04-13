# AtomicIO — NFS-safe atomic write helpers (痛点 #8)

"""
    atomic_write(write_fn, path)

Write `path` atomically: the target either doesn't exist or contains the
complete output. Achieved by `tmp` file + fsync + POSIX rename.

- Parent directory is created if missing.
- If `write_fn` throws, the tmp file is cleaned up and `path` is untouched.
- Safe under NFS (uses `rename`, which is atomic there).

```julia
atomic_write("out/data.jld2") do io
    write(io, payload)
end
```
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
                # fsync not available on this FS; rename is still atomic
            end
        end
        mv(tmp, path; force=true)
    catch
        # Clean up tmp on failure; do NOT touch target path
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

Create an empty file at `path` atomically (for `.done` markers).
"""
atomic_touch(path::AbstractString) = atomic_write(_ -> nothing, path)

export atomic_write, atomic_touch
