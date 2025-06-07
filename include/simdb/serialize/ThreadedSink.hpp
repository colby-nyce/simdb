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
template <typename PipelineDataT = DatabaseEntry>
class ThreadedSink
{
public:
    ThreadedSink(DatabaseManager* db_mgr,
                 EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback,
                 size_t num_compression_threads = 0)
        : db_thread_(db_mgr, end_of_pipeline_callback)
    {
        for (size_t i = 0; i < num_compression_threads; ++i)
        {
            auto thread = std::make_unique<CompressionThread>(compression_queue_, db_thread_);
            sink_threads_.emplace_back(std::move(thread));
        }
    }

    /// Reconfigure the compression level for this thread.
    void setCompressionLevel(CompressionLevel level)
    {
        for (auto& thread : sink_threads_)
        {
            thread->setCompressionLevel(level);
        }
    }

    /// Disable compression for this thread.
    void disableCompression()
    {
        for (auto& thread : sink_threads_)
        {
            thread->disableCompression();
        }
    }

    /// Enable compression for this thread.
    void enableCompression(CompressionLevel compression_level = CompressionLevel::DEFAULT)
    {
        for (auto& thread : sink_threads_)
        {
            thread->enableCompression(compression_level);
        }
    }

    /// Send a new packet down the pipeline. 
    void push(PipelineDataT&& entry)
    {
        compression_queue_.emplace(std::move(entry));
        startThreads_();
    }

    /// Queue arbitrary work to be done on the database thread. This will be
    /// executed inside a BEGIN/COMMIT TRANSACTION block.
    void queueWork(const AnyDatabaseWork& work)
    {
        db_thread_.queueWork(work);
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
            PipelineDataT entry;
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

    ConcurrentQueue<PipelineDataT> compression_queue_;
    DatabaseThread<PipelineDataT> db_thread_;
    std::vector<std::unique_ptr<CompressionThread>> sink_threads_;
    bool threads_running_ = false;
};

} // namespace simdb
