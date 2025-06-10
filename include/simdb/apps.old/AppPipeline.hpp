#pragma once

#include "simdb/pipeline/AsyncPipeline.hpp"

namespace simdb
{

/// App pipelines are preconfigured before instantiating your app.
enum class AppPipelineMode
{
    // Synchronous mode with compression.
    ZERO_STAGES_COMPRESSED,

    // Synchronous mode without compression.
    ZERO_STAGES_UNCOMPRESSED,

    // Use a single background thread for compression and DB writes.
    ONE_STAGE_COMPRESED,

    // Use a single background thread for DB writes, no compression.
    ONE_STAGE_UNCOMPRESSED,

    // Use a thread for compression and a thread for DB writes.
    TWO_STAGES_COMPRESSED
};

/// Optimize performance by using two background threads with compression enabled by default.
inline constexpr auto DEFAULT_APP_PIPELINE_MODE = AppPipelineMode::TWO_STAGES_COMPRESSED;

inline size_t getNumStages(AppPipelineMode mode)
{
    switch (mode)
    {
        case AppPipelineMode::ZERO_STAGES_COMPRESSED:
        case AppPipelineMode::ZERO_STAGES_UNCOMPRESSED:
            return 0;
        case AppPipelineMode::ONE_STAGE_COMPRESED:
        case AppPipelineMode::ONE_STAGE_UNCOMPRESSED:
            return 1;
        case AppPipelineMode::TWO_STAGES_COMPRESSED:
            return 2;
        default:
            throw DBException("Invalid AppPipelineMode: " + std::to_string(static_cast<int>(mode)));
    }
}

inline bool ensureCompressed(AppPipelineMode mode)
{
    switch (mode)
    {
        case AppPipelineMode::ZERO_STAGES_COMPRESSED:
        case AppPipelineMode::ONE_STAGE_COMPRESED:
        case AppPipelineMode::TWO_STAGES_COMPRESSED:
            return true;
        case AppPipelineMode::ZERO_STAGES_UNCOMPRESSED:
        case AppPipelineMode::ONE_STAGE_UNCOMPRESSED:
            return false;
        default:
            throw DBException("Invalid AppPipelineMode: " + std::to_string(static_cast<int>(mode)));
    }
}

class AppPipeline
{
public:
    AppPipeline(AsyncPipeline& async_pipeline, EndOfPipelineCallback end_of_pipeline_callback)
        : async_pipeline_(async_pipeline)
        , end_of_pipeline_callback_(end_of_pipeline_callback)
    {
    }

    const AsyncPipeline& getAsyncPipeline() const
    {
        return async_pipeline_;
    }

    void process(DatabaseEntry&& entry)
    {
        entry.setEndOfPipelineCallback(end_of_pipeline_callback_);
        async_pipeline_.process(std::move(entry));
    }

    void enqueue(std::function<void()> callback, bool fifo = true)
    {
        async_pipeline_.enqueue(std::move(callback), fifo);
    }

    void teardown()
    {
        async_pipeline_.teardown();
    }

private:
    AsyncPipeline& async_pipeline_;
    EndOfPipelineCallback end_of_pipeline_callback_;
    std::vector<char> compressed_data_;
};

} // namespace simdb
