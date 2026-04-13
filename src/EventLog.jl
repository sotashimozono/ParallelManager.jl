# EventLog — structured JSONL event logging (痛点 #7)
#
# Do NOT add a per-item `println` API here. Structured events only.
# The 300MB println logs in FiniteTemperature.jl came from per-item prints;
# this module exists to make that impossible by design.

using Dates
using JSON3

"""
    EventLog(path)

Append-only JSONL event log with thread-safe writes.

Standard event kinds:
  :stage_start, :stage_done
  :key_start, :key_done
  :lock_busy, :lock_reclaimed
  :error, :gave_up, :retry
  :skip_complete

Downstream code should use `log_event` and never `println` directly.
"""
struct EventLog
    path::String
    lock::ReentrantLock
end

EventLog(path::AbstractString) = EventLog(String(path), ReentrantLock())

"""
    log_event(log, kind; kwargs...)

Append one JSON object as a line to `log.path`. Always includes `ts` (ISO-8601)
and `kind` (Symbol). Extra fields come from `kwargs`.
"""
function log_event(log::EventLog, kind::Symbol; kwargs...)
    rec = (; ts=string(now()), kind=String(kind), kwargs...)
    # Build full line with newline and write in a single syscall so O_APPEND
    # preserves line atomicity across processes (multi-master contention).
    line = string(JSON3.write(rec), '\n')
    lock(log.lock) do
        mkpath(dirname(log.path))
        # Open append mode, do one write, close. On POSIX, writes < PIPE_BUF
        # (4096 bytes) with O_APPEND are atomic even across processes.
        open(log.path, "a") do io
            write(io, line)
        end
    end
    return nothing
end

export EventLog, log_event
