#pragma once

#include "simdb/pipeline/AsyncPipeline.hpp"

namespace simdb
{

/// App pipelines are preconfigured before instantiating your app.
enum class AppPipelineMode
{
    // Use a single background thread for compression and DB writes.
    DB_THREAD_ONLY_WITH_COMPRESSION,

    // Use a single background thread for DB writes, no compression.
    DB_THREAD_ONLY_WITHOUT_COMPRESSION,

    // Use a single background thread for DB writes, but compress on the main thread.
    COMPRESS_MAIN_THREAD_THEN_WRITE_DB_THREAD,

    // Use two background threads: one for compression, and another for DB writes.
    COMPRESS_SEPARATE_THREAD_THEN_WRITE_DB_THREAD
};

inline constexpr auto DEFAULT_APP_PIPELINE_MODE = AppPipelineMode::DB_THREAD_ONLY_WITH_COMPRESSION;

class AppPipeline
{
public:
    AppPipeline(AsyncPipeline& async_pipeline, AppPipelineMode mode,
                EndOfPipelineCallback end_of_pipeline_callback)
        : async_pipeline_(async_pipeline)
        , mode_(mode)
        , end_of_pipeline_callback_(end_of_pipeline_callback)
    {
    }

    void process(DatabaseEntry&& entry)
    {
        switch (mode_)
        {
            case AppPipelineMode::DB_THREAD_ONLY_WITHOUT_COMPRESSION:
                entry.unrequireCompression();
                break;

            case AppPipelineMode::DB_THREAD_ONLY_WITH_COMPRESSION:
                entry.requireCompression(!entry.compressed());
                break;

            case AppPipelineMode::COMPRESS_MAIN_THREAD_THEN_WRITE_DB_THREAD:
                entry.compress();
                break;

            case AppPipelineMode::COMPRESS_SEPARATE_THREAD_THEN_WRITE_DB_THREAD:
                entry.requireCompression(!entry.compressed());
                break;
        }

        entry.redirect(end_of_pipeline_callback_);
        async_pipeline_.process(std::move(entry));
    }

    void teardown()
    {
        async_pipeline_.teardown();
    }

    void callLater(std::function<void()> callback)
    {
        async_pipeline_.callLater(callback);
    }

private:
    AsyncPipeline& async_pipeline_;
    AppPipelineMode mode_;
    EndOfPipelineCallback end_of_pipeline_callback_;
    std::vector<char> compressed_data_;
};

} // namespace simdb
