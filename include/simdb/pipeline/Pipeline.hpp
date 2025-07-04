#pragma once

#include "simdb/pipeline/PipelineTaskGroup.hpp"

namespace simdb::pipeline {

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

    std::unique_ptr<TaskGroup> createTaskGroup(const std::string& description = "")
    {
        return std::make_unique<TaskGroup>(pipeline_name_, description);
    }

    void addTaskGroup(std::unique_ptr<TaskGroup> group)
    {
        if (!task_groups_.empty())
        {
            auto prev = task_groups_.back().get();
            prev->setOutputQueue(group->getInputQueue());
        }
        task_groups_.emplace_back(std::move(group));
    }

    void addTaskGroup(std::unique_ptr<TaskBase> task, const std::string& description = "")
    {
        auto group = createTaskGroup(description);
        group->addTask(std::move(task));
        addTaskGroup(std::move(group));
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
