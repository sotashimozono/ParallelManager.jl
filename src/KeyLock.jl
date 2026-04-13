# KeyLock — per-(stage, key) advisory lock via mkdir (痛点 #9, #8)
#
# mkdir is atomic on POSIX filesystems including NFS, so this is the safest
# primitive for multi-master coordination without a central service.
#
# Heartbeat + stale reclaim: a background task touches `heartbeat` while the
# critical section is running. If a master dies, its heartbeat mtime stops
# updating and another master can reclaim the lock after `stale_after` seconds.

"""
    KeyLock(dir; stale_after=600.0, heartbeat_interval=60.0)

Advisory lock rooted at `dir`. `try_acquire` succeeds iff `mkdir(dir)` succeeds
(atomic on POSIX, including NFS).

- `holder` file records `"<hostname>:<pid>"`
- `heartbeat` file mtime is refreshed every `heartbeat_interval` seconds
- a lock is **stale** if `time() - mtime(heartbeat) > stale_after`
- stale locks are automatically reclaimed on contention
"""
struct KeyLock
    dir::String
    holder::String
    stale_after::Float64
    heartbeat_interval::Float64
end

function KeyLock(dir::AbstractString; stale_after::Real=600.0,
                 heartbeat_interval::Real=60.0)
    KeyLock(String(dir),
            string(gethostname(), ":", getpid()),
            Float64(stale_after),
            Float64(heartbeat_interval))
end

holder_path(l::KeyLock)    = joinpath(l.dir, "holder")
heartbeat_path(l::KeyLock) = joinpath(l.dir, "heartbeat")

"""
    touch_heartbeat(l)

Update heartbeat mtime. Safe to call while the lock is held.
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

True iff the lock clearly belongs to a dead holder:
- heartbeat exists and is older than `stale_after`, OR
- heartbeat is missing AND the lock directory itself is older than `stale_after`
  (covers the edge case where the holder died between `mkdir` and first `touch`).

Returns false during the brief window between `mkdir` and the first
`touch(heartbeat)` of a live acquirer — otherwise a concurrent contender
would reclaim a perfectly healthy lock. This is the race that bit the
heartbeat test in todo 06.
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

Forcefully remove `l.dir`. Callers must check `is_stale` first.
Uses rename-then-rm so a concurrent reclaim attempt can't double-delete.
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

Atomically attempt to take the lock. On failure, if the existing lock is
stale, reclaim it once and retry.
"""
function try_acquire(l::KeyLock)::Bool
    mkpath(dirname(l.dir))
    if _mkdir_take(l)
        return true
    end
    # Already held — check if stale
    if is_stale(l)
        reclaim!(l)
        return _mkdir_take(l)
    end
    return false
end

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

Remove the lock directory. Safe to call multiple times.
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

Run `f()` while holding `l` and running a heartbeat task in the background.
Returns `f()`'s value, or `nothing` if the lock could not be acquired.
Releases the lock and stops the heartbeat task even if `f` throws.
"""
function with_key_lock(f::Function, l::KeyLock)
    try_acquire(l) || return nothing
    stop = Threads.Atomic{Bool}(false)
    # Heartbeat loop sleeps in small chunks so `stop` is observed promptly
    # at release — otherwise `wait(hb_task)` blocks for up to heartbeat_interval.
    tick = 0.1
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
