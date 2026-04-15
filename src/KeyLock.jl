# KeyLock — per-(stage, key) advisory lock via `mkdir`.
#
# POSIX guarantees `mkdir(2)` is atomic, including on NFS, so it is the
# simplest lock primitive that works across processes and machines
# without a central service. A background heartbeat task keeps the lock
# "alive" while work is in progress; if a holder dies, the heartbeat
# mtime stops advancing and another master can reclaim the lock after
# `stale_after` seconds.
#
# This module addresses two connected failure modes observed in
# FiniteTemperature.jl:
#   - Two masters double-executing the same key and corrupting output.
#   - A kill -9'd master silently wedging a key that never finishes.

"""
    KeyLock(dir; stale_after=600.0, heartbeat_interval=60.0)

Advisory lock rooted at `dir`.

[`try_acquire`](@ref) succeeds if and only if `mkdir(dir)` succeeds — which
is atomic on POSIX (NFS included). The directory contains two files:

- `holder`    — `"<hostname>:<pid>"` of the current owner
- `heartbeat` — an empty file whose **mtime** is refreshed by a background
                task while the lock is held

A lock is **stale** (see [`is_stale`](@ref)) if:

- `heartbeat` exists and is older than `stale_after` seconds, or
- `heartbeat` is missing and the lock dir itself is older than `stale_after`
  (the holder died between `mkdir` and its first heartbeat write).

Stale locks are automatically reclaimed by the next [`try_acquire`](@ref)
contender — see [`reclaim!`](@ref).

# Fields

- `dir::String` — absolute (or vault-rooted) path of the lock directory.
- `holder::String` — `"host:pid"` string generated at construction.
- `stale_after::Float64` — reclaim threshold in seconds. Default `600.0`
  works well for minute-scale `work_fn`s; reduce for test suites.
- `heartbeat_interval::Float64` — how often `with_key_lock`'s background
  task refreshes `heartbeat`. Default `60.0`. Must be less than
  `stale_after` so healthy locks are never flagged stale.

# NFS considerations

`mkdir` and `rename` are POSIX-atomic on NFS v3+. `flock` is **not**, and
is intentionally not used anywhere in this module. The heartbeat scheme
uses file mtime, which is also NFS-safe (subject to clock skew; in
practice the `stale_after` threshold is much larger than typical skew).

# See also

[`with_key_lock`](@ref), [`try_acquire`](@ref), [`release!`](@ref),
[`is_stale`](@ref), [`reclaim!`](@ref), [`touch_heartbeat`](@ref).
"""
struct KeyLock
    dir::String
    holder::String
    stale_after::Float64
    heartbeat_interval::Float64
end

function KeyLock(
    dir::AbstractString; stale_after::Real=600.0, heartbeat_interval::Real=60.0
)
    KeyLock(
        String(dir),
        string(gethostname(), ":", getpid()),
        Float64(stale_after),
        Float64(heartbeat_interval),
    )
end

"""
    holder_path(l) -> String

`joinpath(l.dir, "holder")` — pure path helper.
"""
holder_path(l::KeyLock) = joinpath(l.dir, "holder")

"""
    heartbeat_path(l) -> String

`joinpath(l.dir, "heartbeat")` — pure path helper.
"""
heartbeat_path(l::KeyLock) = joinpath(l.dir, "heartbeat")

"""
    touch_heartbeat(l)

Update the `heartbeat` file's mtime. Safe to call while the lock is held
or while racing against a reclaim — any filesystem error is swallowed,
since the lock may have been reclaimed out from under us by a contender
that saw it as stale.
"""
function touch_heartbeat(l::KeyLock)
    p = heartbeat_path(l)
    try
        touch(p)
    catch
        # lock dir might have been reclaimed underneath us
    end
    return nothing
end

"""
    is_stale(l) -> Bool

Return `true` if the lock clearly belongs to a dead holder:

- the lock directory does not exist (vacuous true), or
- `heartbeat` exists and is older than `l.stale_after`, or
- `heartbeat` is missing **and** the lock directory itself is older than
  `l.stale_after` — this handles the edge case where the holder died
  between `mkdir` and its first heartbeat write.

Returns `false` during the brief window between `mkdir` and the first
`touch(heartbeat)` of a **live** acquirer. Without this carve-out, a
concurrent contender would race in and reclaim a perfectly healthy lock.
The earlier implementation hit this bug repeatedly in the multi-master
test suite before the dir-age fallback was added.

Pure observation — does not touch the lock dir in any way.
"""
function is_stale(l::KeyLock)::Bool
    isdir(l.dir) || return true  # nothing there → stale (or never held)
    hb = heartbeat_path(l)
    if isfile(hb)
        return time() - mtime(hb) > l.stale_after
    end
    # No heartbeat file yet. Use dir age as fallback — tolerates the
    # short acquisition window but still catches holders that died before
    # the first heartbeat.
    return time() - mtime(l.dir) > l.stale_after
end

"""
    reclaim!(l)

Forcefully remove `l.dir`. Callers must check [`is_stale`](@ref) first.

Uses a rename-then-rm pattern so that two concurrent reclaim attempts do
not both try to `rm` the same directory:

1. `mv(l.dir, "\$(l.dir).dead.\$(getpid()).\$(rand(UInt32))", force=false)`
2. If the `mv` fails (another master already did step 1), return silently.
3. Otherwise `rm` the renamed directory tree.

After this returns, a fresh [`_mkdir_take`](@ref) from the current master
or another contender can succeed.
"""
function reclaim!(l::KeyLock)
    dead = string(l.dir, ".dead.", getpid(), ".", rand(UInt32))
    try
        mv(l.dir, dead; force=false)
    catch
        return nothing  # someone else already reclaimed it
    end
    try
        rm(dead; force=true, recursive=true)
    catch
    end
    return nothing
end

"""
    try_acquire(l) -> Bool

Attempt to acquire the lock.

1. `mkdir(l.dir)` — if it succeeds, write `holder` + `heartbeat` and return `true`.
2. If `mkdir` fails (lock already held), check [`is_stale`](@ref).
3. If stale, [`reclaim!`](@ref) once and retry the mkdir.
4. Otherwise return `false` — another master is legitimately working
   this key and the caller should move on.

Returns `true` on acquisition, `false` if the key is held by a live contender.
Raises nothing on lock contention; all filesystem errors are caught inside.
"""
function try_acquire(l::KeyLock)::Bool
    mkpath(dirname(l.dir))
    if _mkdir_take(l)
        return true
    end
    # Already held — check if stale.
    if is_stale(l)
        reclaim!(l)
        return _mkdir_take(l)
    end
    return false
end

# Internal: the bare `mkdir` + bookkeeping path, without stale handling.
function _mkdir_take(l::KeyLock)::Bool
    try
        mkdir(l.dir)
    catch
        return false
    end
    try
        write(holder_path(l), l.holder)
        touch(heartbeat_path(l))
    catch
        # Could not finalize the lock — roll back.
        try
            rm(l.dir; force=true, recursive=true)
        catch
        end
        return false
    end
    return true
end

"""
    release!(l)

Remove the lock directory if present. Idempotent — safe to call on a lock
that was never acquired, or one that has already been reclaimed by another
master. Any filesystem error is swallowed.
"""
function release!(l::KeyLock)
    try
        rm(l.dir; force=true, recursive=true)
    catch
    end
    return nothing
end

"""
    with_key_lock(f, l)

Run `f()` while holding `l`, with a background heartbeat task keeping the
lock alive. Returns `f()`'s value, or `nothing` if [`try_acquire`](@ref)
failed (the caller should interpret `nothing` as "another master is
handling this key").

The heartbeat task sleeps in 0.1 s ticks and refreshes `heartbeat` every
`l.heartbeat_interval` seconds. Sleeping in small ticks (rather than one
`sleep(heartbeat_interval)`) is what allows `wait(hb_task)` in the finally
block to return promptly after `release!` — an earlier implementation
blocked for up to 60 s per call because Julia cannot interrupt a running
`sleep`.

```julia
klock = KeyLock("locks/phase1/key42"; stale_after=300, heartbeat_interval=30)
result = with_key_lock(klock) do
    do_expensive_work()
end
if result === nothing
    # Another master was handling this key; move on.
end
```

The lock is released and the heartbeat task is stopped even if `f` throws;
the exception is then rethrown. Safe to use inside `Threads.@spawn`.
"""
function with_key_lock(f::Function, l::KeyLock)
    try_acquire(l) || return nothing
    stop = Threads.Atomic{Bool}(false)
    tick = 0.1  # sleep granularity so `stop` is observed within 100 ms
    hb_task = Threads.@spawn begin
        elapsed = 0.0
        while !stop[]
            sleep(tick)
            stop[] && break
            elapsed += tick
            if elapsed >= l.heartbeat_interval
                touch_heartbeat(l)
                elapsed = 0.0
            end
        end
    end
    try
        return f()
    finally
        stop[] = true
        try
            wait(hb_task)
        catch
        end
        release!(l)
    end
end

export KeyLock, try_acquire, release!, with_key_lock
export holder_path, heartbeat_path, touch_heartbeat, is_stale, reclaim!
