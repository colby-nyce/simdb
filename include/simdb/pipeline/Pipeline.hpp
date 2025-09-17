// <Pipeline.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/TaskGroup.hpp"

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// SimDB pipelines are used to create high-performance multi-stage
/// data processors en route to the database. Unlike other pipeline
/// libraries, SimDB enforces move-only semantics for performance.
/// There are no limitations regarding I/O data type changes from
/// one stage/filter/transform to the next.
class Pipeline
{
public:
    Pipeline(DatabaseManager* db_mgr, const std::string& name)
        : db_mgr_(db_mgr)
        , pipeline_name_(name)
    {}

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    std::string getName() const
    {
        return pipeline_name_;
    }

    TaskGroup* createTaskGroup(const std::string& description = "")
    {
        auto group = std::make_unique<TaskGroup>(pipeline_name_, description);
        task_groups_.emplace_back(std::move(group));
        return task_groups_.back().get();
    }

    std::vector<TaskGroup*> getTaskGroups()
    {
        std::vector<TaskGroup*> groups;
        for (auto& group : task_groups_)
        {
            groups.push_back(group.get());
        }
        return groups;
    }

    std::vector<const TaskGroup*> getTaskGroups() const
    {
        std::vector<const TaskGroup*> groups;
        for (auto& group : task_groups_)
        {
            groups.push_back(group.get());
        }
        return groups;
    }

    void waitUntilFlushed(double timeout_sec = 30, uint32_t poll_milli = 10)
    {
        if (poll_milli == 0)
        {
            throw DBException("Pipeline waitUntilFlushed() poll interval must be > 0");
        }

        if (timeout_sec <= 0.0)
        {
            throw DBException("Pipeline waitUntilFlushed() timeout must be > 0");
        }

        while (true)
        {
            bool continue_while = false;
            for (auto& group : task_groups_)
            {
                for (auto& task : group->getTasks())
                {
                    if (auto q = task->getInputQueue())
                    {
                        continue_while |= q->size() > 0;
                    }
                }
            }

            if (continue_while)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(poll_milli));
                timeout_sec -= poll_milli / 1000.0;
                if (timeout_sec <= 0.0)
                {
                    throw DBException("Pipeline waitUntilFlushed() timeout");
                }
            }
            else
            {
                break;
            }
        }
    }

private:
    DatabaseManager* db_mgr_ = nullptr;
    std::string pipeline_name_;
    std::vector<std::unique_ptr<TaskGroup>> task_groups_;
};

} // namespace simdb::pipeline
