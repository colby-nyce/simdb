#pragma once

#include "simdb/pipeline/PipelineTaskGroup.hpp"

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

    TaskGroup* createTaskGroup(const std::string& description = "")
    {
        auto prev = task_groups_.empty() ? nullptr : task_groups_.back().get();
        auto group = std::make_unique<TaskGroup>(pipeline_name_, prev, description);
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

    template <typename Input>
    ConcurrentQueue<Input>* getPipelineInput()
    {
        if (task_groups_.empty())
        {
            return nullptr;
        }

        return task_groups_[0]->getPipelineInput<Input>();
    }

    bool requiresDatabase() const
    {
        for (const auto& group : task_groups_)
        {
            if (group->requiresDatabase())
            {
                return true;
            }
        }
        return false;
    }

private:
    DatabaseManager* db_mgr_ = nullptr;
    std::string pipeline_name_;
    std::vector<std::unique_ptr<TaskGroup>> task_groups_;
};

} // namespace simdb::pipeline
