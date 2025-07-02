#pragma once

#include "simdb/pipeline/PipelineRunnable.hpp"
#include "simdb/pipeline/PipelineThread.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"

namespace simdb::pipeline {

template <typename DatabaseIn>
class DatabaseQueue : public Runnable
{
public:
    using DatabaseFunc = std::function<void(DatabaseIn&&, DatabaseManager*)>;

    DatabaseQueue(DatabaseThread& db_thread, DatabaseFunc db_func)
        : db_func_(db_func)
        , db_mgr_(db_thread.getDatabaseManager())
    {
        db_thread.addRunnable(this);
    }

    void process(DatabaseIn&& in)
    {
        input_queue_.emplace(std::move(in));
    }

private:
    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (input_queue_.try_pop(in))
        {
            db_func_(std::move(in), db_mgr_);
            ran = true;
        }
        return ran;
    }

    DatabaseFunc db_func_;
    DatabaseManager* db_mgr_ = nullptr;
    ConcurrentQueue<DatabaseIn> input_queue_;
};

} // namespace simdb::pipeline
