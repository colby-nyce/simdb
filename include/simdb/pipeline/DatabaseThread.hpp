#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/pipeline/DatabaseEntry.hpp"
#include <any>

namespace simdb
{

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
        entry.redirect(f);
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
            entry.compress();

            if (auto cb = entry.getReroutedCallback())
            {
                cb(entry.getDatabaseManager());
            }
            else if (auto cb = entry.getEndOfPipelineCallback())
            {
                cb(std::move(entry));
            }
        };

        DatabaseEntry entry;
        while (queue_.try_pop(entry))
        {
            auto db_mgr = entry.getDatabaseManager();
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

    ConcurrentQueue<DatabaseEntry> queue_;
    std::vector<char> compressed_data_;
};

} // namespace simdb
