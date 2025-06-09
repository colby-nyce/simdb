#pragma once

#include "simdb/pipeline/CompressionThread.hpp"

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
class AsyncPipeline
{
public:
    // Create a pipeline with 0, 1, or 2 stages:
    //   0 stages: Synchronous mode
    //   1 stage:  DB thread only
    //   2 stages: DB thread + CompressionThread
    //
    // Examples:
    //   - 0 stages compressed
    //     --> compress main thread and process immediately
    //   - 1 stage compressed
    //     --> compress and write on DB thread
    //   - 2 stages compressed
    //     --> compress on CompressionThread and write on DB thread
    AsyncPipeline(size_t num_stages = 1, bool ensure_compressed = true)
    {
        if (num_stages == 2 && !ensure_compressed)
        {
            throw DBException("AsyncPipeline with 2 stages must ensure compression.");
        }
        else if (num_stages == 2)
        {
            compression_thread_ = std::make_unique<CompressionThread>(compression_queue_, db_thread_);
        }
        else if (num_stages == 0)
        {
            db_thread_.useSynchronousMode();
        }
        else if (num_stages != 1)
        {
            throw DBException("Invalid number of stages in AsyncPipeline (max 2): " + std::to_string(num_stages));
        }

        db_thread_.ensureCompressed(ensure_compressed);
    }

    /// Check if packets are guaranteed to be compressed.
    bool isEnsuredCompressed() const
    {
        return db_thread_.isEnsuredCompressed();
    }

    /// Send a new packet down the pipeline.
    void process(DatabaseEntry&& entry)
    {
        if (compression_thread_)
        {
            compression_queue_.emplace(std::move(entry));
            startThreads_();
        }
        else
        {
            db_thread_.process(std::move(entry));
        }
    }

    void callLater(std::function<void()> callback)
    {
        // TODO cnyce: Look into whether it should be queued in stage 1
        db_thread_.callLater(callback);
    }

    /// Flush the pipeline, allowing all threads to finish their work. This
    /// occurs periodically on the background thread even if you do not call
    /// this method explicitly.
    void flush()
    {
        if (compression_thread_)
        {
            // Allow the threads to finish their work.
            while (!compression_queue_.empty())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        db_thread_.waitUntilFlushed();
    }

    /// Stop all threads and flush the pipeline.
    void teardown()
    {
        flush();

        // Stop the compression threads.
        compression_thread_.reset();

        // Flush and stop the database thread.
        db_thread_.teardown();
    }

private:
    void startThreads_()
    {
        if (!threads_running_ && compression_thread_)
        {
            compression_thread_->startThreadLoop();
            threads_running_ = true;
        }
    }

    ConcurrentQueue<DatabaseEntry> compression_queue_;
    DatabaseThread db_thread_;
    std::unique_ptr<CompressionThread> compression_thread_;
    bool threads_running_ = false;
};

} // namespace simdb
