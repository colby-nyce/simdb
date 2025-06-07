#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"
#include <any>

namespace simdb
{

template <typename PipelineDataT>
using EndOfPipelineCallback = std::function<void(PipelineDataT&&)>;

#define END_OF_PIPELINE_CALLBACK(ApplicationType, method_name) \
    std::bind(&ApplicationType::method_name, dynamic_cast<ApplicationType*>(__this__), \
              std::placeholders::_1)

struct DatabaseEntry
{
    uint64_t tick = 0;
    const void* data_ptr = nullptr;
    size_t num_bytes = 0;
    bool compressed = false;
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
};

class DatabaseManager;

/// Use this class to send data to the database in a separate thread.
/// DatabaseThread does not make assumptions about what you want to
/// do with the data (e.g. it doesn't know about specific tables) so
/// you need to provide a callback that will be called on the background
/// thread.
///
/// The "ProcessInTransaction" template parameter allows you to control
/// whether the data will be processed in a BEGIN/COMMIT TRANSACTION block.
/// If your callback intends to write data to the database, it is strongly
/// recommended to set this to true, as it will ensure that the data is
/// written atomically, which is important for performance and data integrity.
template <bool ProcessInTransaction = true>
class DatabaseThread : public Thread
{
public:
    DatabaseThread(EndOfPipelineCallback<DatabaseEntry> end_of_pipeline_callback,
                   const double interval_seconds = 0.5)
        : Thread(interval_seconds * 1000)
        , end_of_pipeline_callback_(end_of_pipeline_callback)
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
        flush();
        stopThreadLoop();
    }

    /// Flush the pipeline.
    void flush()
    {
        if constexpr (ProcessInTransaction)
        {
            // Since there is only one DatabaseThread which can serve more than one
            // DatabaseManager, we need to ensure that we call safeTransaction() on
            // the appropriate database, and only forward the data that belongs to
            // that database.
            std::unordered_map<DatabaseManager*, std::queue<DatabaseEntry>> db_queues;
            DatabaseEntry entry;
            while (queue_.try_pop(entry))
            {
                db_queues[entry.db_mgr].emplace(std::move(entry));
            }

            auto process_entries = [this](std::queue<DatabaseEntry>& entries)
            {
                while (!entries.empty())
                {
                    auto& entry = entries.front();
                    if (entry.rerouted_callback)
                    {
                        entry.rerouted_callback(entry.db_mgr);
                    }
                    else
                    {
                        end_of_pipeline_callback_(std::move(entry));
                    }
                    entries.pop();
                }
            };

            for (auto& [db_mgr, entries] : db_queues)
            {
                if (db_mgr)
                {
                    db_mgr->safeTransaction([&]() { process_entries(entries); });
                }
                else
                {
                    process_entries(entries);
                }
            }
        }
        else
        {
            DatabaseEntry entry;
            while (queue_.try_pop(entry))
            {
                if (entry.rerouted_callback)
                {
                    entry.rerouted_callback(entry.db_mgr);
                }
                else
                {
                    end_of_pipeline_callback_(std::move(entry));
                }
            }
        }
    }

private:
    /// Called periodically on the background thread to flush all pending data.
    void onInterval_() override
    {
        flush();
    }

    ConcurrentQueue<DatabaseEntry> queue_;
    EndOfPipelineCallback<DatabaseEntry> end_of_pipeline_callback_;
};

} // namespace simdb
