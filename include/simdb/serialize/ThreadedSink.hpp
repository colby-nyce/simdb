#pragma once

#include "simdb/serialize/DatabaseThread.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

/// One or more of these threads work on the ThreadedSink's queue of pending
/// DatabaseEntry objects. Each of these threads can have its own compression
/// or no compression at all. They individually regulate their own internals
/// in response to their % of the total work load (higher/lower/no compression).
class SinkThread : public Thread
{
public:
    SinkThread(ConcurrentQueue<DatabaseEntry>& queue, DatabaseThread& db_thread)
        : Thread(500)
        , queue_(queue)
        , db_thread_(db_thread)
    {
    }

private:
    /// Called every 500ms. Flush whatever we can from the queue, compress it,
    /// and send it to the database thread. Remember that this queue is a shared
    /// reference across all SinkThread objects (and is owned by the ThreadedSink).
    void onInterval_() override
    {
        DatabaseEntry entry;
        while (queue_.try_pop(entry))
        {
            compress_(entry);
            db_thread_.push(std::move(entry));
        }
    }

    /// Compress the entry if we are able.
    void compress_(DatabaseEntry& entry)
    {
        if (entry.compressed)
        {
            return;
        }

        compressDataVec(entry.bytes, compressed_bytes_, 1);
        std::swap(entry.bytes, compressed_bytes_);
        entry.compressed = true;
    }

    ConcurrentQueue<DatabaseEntry>& queue_;
    DatabaseThread& db_thread_;
    std::vector<char> compressed_bytes_;
};

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
/// SinkThreads plus the DatabaseThread.
class ThreadedSink
{
public:
    ThreadedSink(DatabaseManager* db_mgr, size_t num_compression_threads = 1)
        : db_thread_(db_mgr)
    {
        for (size_t i = 0; i < num_compression_threads; ++i)
        {
            auto thread = std::make_unique<SinkThread>(compression_queue_, db_thread_);
            sink_threads_.emplace_back(std::move(thread));
        }
    }

    void push(DatabaseEntry&& entry)
    {
        compression_queue_.emplace(std::move(entry));
        startThreads_();
    }

    void queueWork(const AnyDatabaseWork& work)
    {
        db_thread_.queueWork(work);
    }

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
    DatabaseThread db_thread_;
    std::vector<std::unique_ptr<SinkThread>> sink_threads_;
    bool threads_running_ = false;
};

} // namespace simdb
