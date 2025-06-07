#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

template <typename PipelineDataT>
using EndOfPipelineCallback = std::function<void(DatabaseManager*, PipelineDataT&&)>;

#define END_OF_PIPELINE_CALLBACK(ApplicationType, method_name) \
    std::bind(&ApplicationType::method_name, dynamic_cast<ApplicationType*>(__this__), \
              std::placeholders::_1, std::placeholders::_2)

template <typename T=char>
struct DatabaseEntry
{
    std::vector<T> bytes;
    bool compressed = false;
    uint64_t tick = 0;
};

class DatabaseManager;

/// This class serves as the last stage in a database pipeline.
template <typename PipelineDataT>
class DatabaseThread : public Thread
{
public:
    DatabaseThread(DatabaseManager* db_mgr, EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback)
        : Thread(500)
        , db_mgr_(db_mgr)
        , end_of_pipeline_callback_(end_of_pipeline_callback)
    {
    }

    void push(PipelineDataT&& entry)
    {
        queue_.emplace(std::move(entry));
        startThreadLoop();
    }

    void teardown()
    {
        flush();
        stopThreadLoop();
    }

    void flush()
    {
        db_mgr_->safeTransaction(
            [&]()
            {
                PipelineDataT entry;
                while (queue_.try_pop(entry))
                {
                    end_of_pipeline_callback_(db_mgr_, std::move(entry));
                }
                return true;
            });
    }

private:
    void onInterval_() override
    {
        flush();
    }

    ConcurrentQueue<PipelineDataT> queue_;
    DatabaseManager* db_mgr_;
    EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback_;
};

} // namespace simdb
