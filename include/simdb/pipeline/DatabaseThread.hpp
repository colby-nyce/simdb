#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"
#include "simdb/utils/Compress.hpp"
#include <any>

namespace simdb
{

struct DatabaseEntry;

using EndOfPipelineCallback = std::function<void(DatabaseEntry&&)>;

#define END_OF_PIPELINE_CALLBACK(Class, Method) \
    std::bind(&Class::Method, dynamic_cast<Class*>(__this__), std::placeholders::_1)


struct DatabaseEntry
{
    uint64_t tick = 0;
    const void* data_ptr = nullptr;
    size_t num_bytes = 0;
    bool compressed = false;
    bool requires_compression = false;
    DatabaseManager* db_mgr = nullptr;

    // This can hold any type of contiguous data, such as
    // std::vector<char> or std::array<T, N>.
    std::any container;

    // By default, the DatabaseThread will send the data entry
    // to the user-provided callback to handle the data. Some
    // users may want to selectively override this behavior
    // and reroute the data to a different callback. If provided,
    // this callback will be called with the DatabaseManager
    // that was originally set in the DatabaseEntry.
    std::function<void(DatabaseManager* db_mgr)> rerouted_callback = nullptr;

    // Allow applications to set the end-of-pipeline callback.
    EndOfPipelineCallback end_of_pipeline_callback = nullptr;
};

class DatabaseManager;

/// Use this class to send data to the database in a separate thread.
class DatabaseThread : public Thread
{
public:
    DatabaseThread(const double interval_seconds = 0.5)
        : Thread(interval_seconds * 1000)
    {
    }

    /// Send a new packet down the pipeline.
    void process(DatabaseEntry&& entry)
    {
        queue_.emplace(std::move(entry));
        startThreadLoop();
    }

    /// Put any work that needs to be done later in the pipeline.
    /// This will still be processed on the background thread,
    /// in the same order as the other entries in the queue.
    ///
    ///  Main thread:                                Background thread:
    ///    process(...data...);                        // (1)
    ///    process(...data...);                        // (2)
    ///    callLater([]() { ...do something... });     // (3)
    ///    process(...data...);                        // (4)
    ///
    /// This is done to ensure that everything is processed as FIFO to
    /// help keep the asynchronous nature of the pipeline deterministic.
    void callLater(std::function<void()> callback)
    {
        auto f = [callback](DatabaseManager*)
        {
            callback();
        };

        DatabaseEntry entry;
        entry.db_mgr = nullptr;
        entry.rerouted_callback = f;
        process(std::move(entry));
    }

    /// Flush the pipeline and stop the thread.
    void teardown()
    {
        waitUntilFlushed();
        stopThreadLoop();
    }

    /// Wait for the pipeline to be flushed.
    void waitUntilFlushed()
    {
        while (!queue_.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

private:
    /// Called periodically on the background thread to flush all pending data.
    void onInterval_() override
    {
        // Since there is only one DatabaseThread which can serve more than one
        // DatabaseManager, we need to ensure that we call safeTransaction() on
        // the appropriate database, and only forward the data that belongs to
        // that database.
        std::vector<DatabaseManager*> db_managers;
        flush_(db_managers);
    }

    /// Flush the queue and process all entries. 
    void flush_(std::vector<DatabaseManager*>& db_managers)
    {
        auto process_entry = [&](DatabaseEntry& entry)
        {
            if (!entry.compressed && entry.requires_compression)
            {
                compress_(entry);
            }

            if (entry.rerouted_callback)
            {
                entry.rerouted_callback(entry.db_mgr);
            }
            else if (entry.end_of_pipeline_callback)
            {
                auto callback = entry.end_of_pipeline_callback;
                callback(std::move(entry));
            }
        };

        DatabaseEntry entry;
        while (queue_.try_pop(entry))
        {
            auto db_mgr = entry.db_mgr;
            if (db_mgr && std::find(db_managers.begin(), db_managers.end(), db_mgr) == db_managers.end())
            {
                db_mgr->safeTransaction([&]()
                {
                    process_entry(entry);
                    db_managers.push_back(db_mgr);
                    flush_(db_managers);
                });
            }
            else
            {
                process_entry(entry);
            }
        }
    }

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
    }

    ConcurrentQueue<DatabaseEntry> queue_;
    std::vector<char> compressed_data_;
    //EndOfPipelineCallback end_of_pipeline_callback_;
};

} // namespace simdb
