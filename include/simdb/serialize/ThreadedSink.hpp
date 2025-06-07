#pragma once

#include "simdb/serialize/CompressionThread.hpp"

namespace simdb
{

/// This class holds onto a configurable number of threads that work on
/// the ever-growing queue of DatabaseEntry objects given to us. These
/// threads grab whatever they can from the queue, compress the data and
/// send it to the database thread.
///
/// Note that SimDB requires the use of a background thread to write data.
/// This is due to performance guarantees that SimDB wants to provide. On
/// a background thread, we can guarantee sensible use of atomic BEGIN/
/// COMMIT TRANSACTION blocks without touching the file system unnecessarily
/// in the main thread.
///
/// All that to say that the total number of threads is the number of
/// CompressionThreads plus the DatabaseThread.
template <typename PipelineDataT = char>
class ThreadedSink
{
public:
    using DatabaseEntry = simdb::DatabaseEntry<PipelineDataT>;

    ThreadedSink(DatabaseManager* db_mgr,
                 EndOfPipelineCallback<DatabaseEntry> end_of_pipeline_callback,
                 size_t num_compression_threads = 0)
        : db_thread_(db_mgr, end_of_pipeline_callback)
    {
        for (size_t i = 0; i < num_compression_threads; ++i)
        {
            auto thread = std::make_unique<CompressionThread<PipelineDataT>>(compression_queue_, db_thread_);
            sink_threads_.emplace_back(std::move(thread));
        }
    }

    /// How many compression threads are running?
    size_t numCompressionThreads() const
    {
        return sink_threads_.size();
    }

    /// Reconfigure the compression level for this thread. Optionally specify which
    /// compression thread ("stage"), or -1 for all threads.
    void setCompressionLevel(CompressionLevel level, int stage = -1)
    {
        if (stage >= static_cast<int>(sink_threads_.size()))
        {
            throw std::out_of_range("Invalid compression thread stage: " + std::to_string(stage));
        }

        for (int idx = 0; idx < static_cast<int>(sink_threads_.size()); ++idx)
        {
            if (stage != -1 && idx != stage)
            {
                continue; // Skip this thread if we're not setting it.
            }
            sink_threads_[idx]->setCompressionLevel(level);
            if (stage != -1)
            {
                break; // Only set the level for the specified thread.
            }
        }
    }

    /// Disable compression for this thread. Optionally specify which
    /// compression thread ("stage"), or -1 for all threads.
    void disableCompression(int stage = -1)
    {
        if (stage >= static_cast<int>(sink_threads_.size()))
        {
            throw std::out_of_range("Invalid compression thread stage: " + std::to_string(stage));
        }

        for (int idx = 0; idx < static_cast<int>(sink_threads_.size()); ++idx)
        {
            if (stage != -1 && idx != stage)
            {
                continue; // Skip this thread if we're not setting it.
            }
            sink_threads_[idx]->disableCompression();
            if (stage != -1)
            {
                break; // Only set the level for the specified thread.
            }
        }
    }

    /// Enable compression for this thread. Optionally specify which
    /// compression thread ("stage"), or -1 for all threads.
    void enableCompression(CompressionLevel level = CompressionLevel::DEFAULT, int stage = -1)
    {
        if (stage >= static_cast<int>(sink_threads_.size()))
        {
            throw std::out_of_range("Invalid compression thread stage: " + std::to_string(stage));
        }

        for (int idx = 0; idx < static_cast<int>(sink_threads_.size()); ++idx)
        {
            if (stage != -1 && idx != stage)
            {
                continue; // Skip this thread if we're not setting it.
            }
            sink_threads_[idx]->enableCompression(level);
            if (stage != -1)
            {
                break; // Only set the level for the specified thread.
            }
        }
    }

    /// Send a new packet down the pipeline. 
    void push(DatabaseEntry&& entry)
    {
        compression_queue_.emplace(std::move(entry));
        startThreads_();
    }

    /// Flush the pipeline, allowing all threads to finish their work. This
    /// occurs periodically on the background thread even if you do not call
    /// this method explicitly.
    void flush()
    {
        if (!sink_threads_.empty())
        {
            // Allow the threads to finish their work.
            while (!compression_queue_.empty())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        else
        {
            // Send uncompressed data directly to the database thread.
            DatabaseEntry entry;
            while (compression_queue_.try_pop(entry))
            {
                db_thread_.push(std::move(entry));
            }
        }

        db_thread_.flush();
    }

    /// Stop all threads and flush the pipeline.
    void teardown()
    {
        flush();

        // Stop the compression threads.
        sink_threads_.clear();

        // Flush and stop the database thread.
        db_thread_.teardown();
    }

private:
    void startThreads_()
    {
        if (!threads_running_)
        {
            for (auto& thread : sink_threads_)
            {
                thread->startThreadLoop();
            }
            threads_running_ = true;
        }
    }

    ConcurrentQueue<DatabaseEntry> compression_queue_;
    DatabaseThread<DatabaseEntry> db_thread_;
    std::vector<std::unique_ptr<CompressionThread<PipelineDataT>>> sink_threads_;
    bool threads_running_ = false;
};

} // namespace simdb
