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
template <typename PipelineDataT, bool ProcessInTransaction = true>
class DatabaseThread : public Thread
{
public:
    DatabaseThread(EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback,
                   const double interval_seconds = 0.5)
        : Thread(interval_seconds * 1000)
        , end_of_pipeline_callback_(end_of_pipeline_callback)
    {
    }

    /// Send a new packet down the pipeline.
    void process(PipelineDataT&& entry)
    {
        queue_.emplace(std::move(entry));
        startThreadLoop();
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
            std::unordered_map<DatabaseManager*, std::queue<PipelineDataT>> db_queues;
            PipelineDataT entry;
            while (queue_.try_pop(entry))
            {
                if (!entry.db_mgr)
                {
                    throw DBException("DatabaseEntry does not have a valid DatabaseManager.");
                }
                db_queues[entry.db_mgr].emplace(std::move(entry));
            }

            for (auto& [db_mgr, entries] : db_queues)
            {
                db_mgr->safeTransaction(
                    [&entries, this]()
                    {
                        while (!entries.empty())
                        {
                            end_of_pipeline_callback_(std::move(entries.front()));
                            entries.pop();
                        }
                    });
            }
        }
        else
        {
            PipelineDataT entry;
            while (queue_.try_pop(entry))
            {
                end_of_pipeline_callback_(std::move(entry));
            }
        }
    }

private:
    /// Called periodically on the background thread to flush all pending data.
    void onInterval_() override
    {
        flush();
    }

    ConcurrentQueue<PipelineDataT> queue_;
    EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback_;
};

} // namespace simdb
