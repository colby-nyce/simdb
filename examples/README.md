# SimDB Pipeline Examples

Each pipeline example showcases a subset of SimDB pipeline features and how to use them.

---

## [SimplePipeline](SimplePipeline/)

Basic pipeline - get started here for an intro to SimDB pipelines. Shows how to:

- Create single-input, single-output stages
- Create single-input, zero-output stages
- Create a pipeline flusher

## [MultiPortStages](MultiPortStages/)

Shows how to:

- Create multi-input, multi-output stages
- Create multi-input, zero-output stages
- Create muxers: only send pair/tuple of values to next stage when all inputs are available

## [ConcurrentApps](ConcurrentApps/)

Shows how to:

- Run multiple instances of the same app

## [DatabaseWatchdog](DatabaseWatchdog/)

Shows how to:

- Create zero-input, zero-output stages
- Create a pipeline flusher
- Temporarily disable pipelines using RAII utility
- Run DB queries on the dedicated DB thread from any non-DB thread

