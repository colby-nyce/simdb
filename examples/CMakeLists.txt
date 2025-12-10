project(simdb_examples)

# Basic pipeline - get started here for an intro to SimDB pipelines.
add_subdirectory(SimplePipeline)

# Multiple input/output ports (queues) for pipeline stages.
add_subdirectory(MultiPortStages)

# Run more than one instance of an app.
add_subdirectory(ConcurrentApps)
