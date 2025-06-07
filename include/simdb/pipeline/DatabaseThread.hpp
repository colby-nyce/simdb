#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"
#include <any>

namespace simdb
{

template <typename PipelineDataT>
using EndOfPipelineCallback = std::function<void(DatabaseManager*, PipelineDataT&&)>;

#define END_OF_PIPELINE_CALLBACK(ApplicationType, method_name) \
    std::bind(&ApplicationType::method_name, dynamic_cast<ApplicationType*>(__this__), \
              std::placeholders::_1, std::placeholders::_2)

struct DatabaseEntry
{
    uint64_t tick = 0;
    const void* data_ptr = nullptr;
    size_t num_bytes = 0;
    bool compressed = false;

    // This can hold any type of contiguous data, such as
    // std::vector<char> or std::array<T, N>.
    std::any container;
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
