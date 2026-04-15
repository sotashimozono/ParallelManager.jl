# API reference

## AtomicIO

```@docs
ParallelManager.atomic_write
ParallelManager.atomic_touch
```

## EventLog

```@docs
ParallelManager.EventLog
ParallelManager.log_event
```

## Manifest

```@docs
ParallelManager.Manifest
ParallelManager.manifest_path
ParallelManager.load_manifest
ParallelManager.save_manifest
ParallelManager.add_complete!
ParallelManager.is_complete
ParallelManager.todo_keys
ParallelManager.manifest_root
```

## KeyLock

```@docs
ParallelManager.KeyLock
ParallelManager.try_acquire
ParallelManager.release!
ParallelManager.with_key_lock
ParallelManager.touch_heartbeat
ParallelManager.is_stale
ParallelManager.reclaim!
ParallelManager.holder_path
ParallelManager.heartbeat_path
```

## InitWorkers

```@docs
ParallelManager.init_workers!
ParallelManager.detect_mode
```

## Run

```@docs
ParallelManager.RunOpts
ParallelManager.run!
```
