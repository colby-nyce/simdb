// <TaskGroup.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Task.hpp"

namespace simdb::pipeline {

/// The TaskGroup class is used to inform the pipeline which tasks should
/// be run on the same thread together:
///
///   TaskGroup1             (thread 1)
///     Task1
///     Task2
///   ----------------------------------
///   TaskGroup2             (thread 2)
///     Task1
///
/// Note that it does NOT tell the pipeline "create a thread just for me
/// and my tasks". In the event of multiple running apps/pipelines, SimDB
/// may try to share threads for some TaskGroups.
///
class TaskGroup : public Runnable
{
public:
    TaskGroup(const std::string& pipeline_name, const std::string& description = "")
        : pipeline_name_(pipeline_name)
        , description_(description)
    {}

    TaskGroup* addTask(std::unique_ptr<TaskBase> task, const std::string& description = "")
    {
        std::string task_desc = pipeline_name_ + "." + task->getDescription();
        if (!description.empty())
        {
            task_desc += " (" + description + ")";
        }
        task->setDescription(task_desc);

        requires_db_ |= task->requiresDatabase();
        tasks_.emplace_back(std::move(task));
        return this;
    }

    std::vector<TaskBase*> getTasks()
    {
        std::vector<TaskBase*> tasks;
        for (auto& task : tasks_)
        {
            tasks.push_back(task.get());
        }
        return tasks;
    }

    bool requiresDatabase() const
    {
        return requires_db_;
    }

    void setDatabaseManager(DatabaseManager* db_mgr)
    {
        assert(requiresDatabase());

        for (auto& task : tasks_)
        {
            if (task->requiresDatabase())
            {
                task->setDatabaseManager(db_mgr);
            }
        }
    }

    void print(std::ostream& os, int indent = 0) const override
    {
        Runnable::print(os, indent);
        for (const auto& task : tasks_)
        {
            task->print(os, indent + 4);
        }
    }

    bool run() override
    {
        bool ran = false;
        for (auto& task : tasks_)
        {
            ran |= task->run();
        }
        return ran;
    }

private:
    std::string getDescription_() const override
    {
        std::string name = "TaskGroup for pipeline '" + pipeline_name_ + "'";
        if (!description_.empty())
        {
            name += " (" + description_ + ")";
        }
        return name;
    }

    std::string pipeline_name_;
    std::string description_;
    std::vector<std::unique_ptr<TaskBase>> tasks_;
    bool requires_db_ = false;
};

} // namespace simdb::pipeline
