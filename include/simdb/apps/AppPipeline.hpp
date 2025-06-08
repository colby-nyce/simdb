#pragma once

#include "simdb/pipeline/AsyncPipeline.hpp"
#include "simdb/utils/Compress.hpp"

namespace simdb
{

/// App pipelines are preconfigured before instantiating your app.
/// The default mode is DB_THREAD_ONLY_WITHOUT_COMPRESSION.
enum class AppPipelineMode
{
    // Use a single background thread for DB writes, no compression.
    DB_THREAD_ONLY_WITHOUT_COMPRESSION,

    // Use a single background thread for compression and DB writes.
    DB_THREAD_ONLY_WITH_COMPRESSION,

    // Use a single background thread for DB writes, but compress on the main thread.
    COMPRESS_MAIN_THREAD_THEN_WRITE_DB_THREAD,

    // Use two background threads: one for compression, and another for DB writes.
    COMPRESS_SEPARATE_THREAD_THEN_WRITE_DB_THREAD
};

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
                entry.requires_compression = false;
                break;

            case AppPipelineMode::DB_THREAD_ONLY_WITH_COMPRESSION:
                entry.requires_compression = !entry.compressed;
                break;

            case AppPipelineMode::COMPRESS_MAIN_THREAD_THEN_WRITE_DB_THREAD:
                compress_(entry);
                break;

            case AppPipelineMode::COMPRESS_SEPARATE_THREAD_THEN_WRITE_DB_THREAD:
                entry.requires_compression = !entry.compressed;
                break;
        }

        entry.end_of_pipeline_callback = end_of_pipeline_callback_;
        async_pipeline_.process(std::move(entry));
    }

    void teardown()
    {
        async_pipeline_.teardown();
    }

private:
    void compress_(DatabaseEntry& entry)
    {
        if (entry.compressed)
        {
            entry.requires_compression = false;
            return;
        }

        if (entry.data_ptr == nullptr || entry.num_bytes == 0)
        {
            throw DBException("Cannot compress empty data.");
        }

        compressDataVec(entry.data_ptr, entry.num_bytes, compressed_data_);
        entry.data_ptr = compressed_data_.data();
        entry.num_bytes = compressed_data_.size();
        entry.container = std::move(compressed_data_);
        entry.compressed = true;
        entry.requires_compression = false;
    }

    AsyncPipeline& async_pipeline_;
    AppPipelineMode mode_;
    EndOfPipelineCallback end_of_pipeline_callback_;
    std::vector<char> compressed_data_;
};

} // namespace simdb
