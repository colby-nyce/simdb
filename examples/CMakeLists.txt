project(simdb_examples)

# Basic pipeline - get started here for an intro to SimDB pipelines.
add_subdirectory(SimplePipeline)

# Multiple input/output ports (queues) for pipeline stages.
add_subdirectory(MultiPortStages)

# Run more than one instance of an app, and/or run 2+ different apps concurrently.
add_subdirectory(ConcurrentApps)

# TODO cnyce: add comments above what features are shown, e.g. async queries or flushing.
