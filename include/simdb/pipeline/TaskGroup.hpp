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

    std::vector<const TaskBase*> getTasks() const
    {
        std::vector<const TaskBase*> tasks;
        for (auto& task : tasks_)
        {
            tasks.push_back(task.get());
        }
        return tasks;
    }

    void print(std::ostream& os, int indent = 0) const override
    {
        Runnable::print(os, indent);
        for (const auto& task : tasks_)
        {
            task->print(os, indent + 4);
        }
    }

private:
    bool run(bool force_flush) override
    {
        bool ran = false;
        bool task_ran = false;
        do
        {
            task_ran = false;
            for (auto& task : tasks_)
            {
                task_ran |= task->run(force_flush);
            }

            ran |= task_ran;
        } while(task_ran);

        return ran;
    }

    bool flushToPipeline() override
    {
        bool did_work = Runnable::flushToPipeline();
        for (auto& task : tasks_)
        {
            did_work |= task->flushToPipeline();
        }
        return did_work;
    }

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
};

} // namespace simdb::pipeline
