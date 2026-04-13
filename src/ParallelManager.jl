module ParallelManager

# Implementation is split across files. Each file is added as it gets implemented
# in the todo sequence (see work/dev/HPCformat/todo/).
#
# Do NOT add a per-item `println` API anywhere in this module. Structured events
# go through EventLog (JSONL) only. This is a structural answer to痛点 #7.

include("AtomicIO.jl")
include("EventLog.jl")
include("Manifest.jl")
include("KeyLock.jl")
include("InitWorkers.jl")
include("Run.jl")

end # module ParallelManager
