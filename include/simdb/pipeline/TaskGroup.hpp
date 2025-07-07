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
    TaskGroup(const std::string& pipeline_name, TaskGroup* prev = nullptr, const std::string& description = "")
        : pipeline_name_(pipeline_name)
        , prev_group_(prev)
        , description_(description)
    {}

    TaskGroup* addTask(std::unique_ptr<TaskBase> task, const std::string& description = "")
    {
        std::string task_name = pipeline_name_ + "." + task->getName();
        if (!description.empty())
        {
            task_name += " (" + description + ")";
        }
        task->setName(task_name);

        TaskBase* prev_task = nullptr;
        if (!tasks_.empty())
        {
            prev_task = tasks_.back().get();
        }
        else if (prev_group_ && !prev_group_->tasks_.empty())
        {
            prev_task = prev_group_->tasks_.back().get();
        }

        if (prev_task)
        {
            prev_task->setOutputQueue(task->getInputQueue());
        }

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

    QueueBase* getInputQueue()
    {
        if (tasks_.empty())
        {
            return nullptr;
        }
        return tasks_[0]->getInputQueue();
    }

    void setOutputQueue(QueueBase* queue)
    {
        if (tasks_.empty())
        {
            throw DBException("Cannot set output queue - no tasks assigned to group");
        }
        tasks_.back()->setOutputQueue(queue);
    }

    bool requiresDatabase() const
    {
        return requires_db_;
    }

    void setDatabaseManager(DatabaseManager* db_mgr)
    {
        for (auto& task : tasks_)
        {
            if (task->requiresDatabase())
            {
                task->setDatabaseManager(db_mgr);
            }
        }
    }

    template <typename Input>
    ConcurrentQueue<Input>* getPipelineInput()
    {
        if (tasks_.empty())
        {
            return nullptr;
        }

        auto task = tasks_[0].get();
        auto queue = task->getInputQueue();
        if (auto q = dynamic_cast<Queue<Input>*>(queue))
        {
            return &q->get();
        }
        return nullptr;
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
    std::string getName_() const override
    {
        std::string name = "TaskGroup for pipeline '" + pipeline_name_ + "'";
        if (!description_.empty())
        {
            name += " (" + description_ + ")";
        }
        return name;
    }

    std::string pipeline_name_;
    TaskGroup* prev_group_ = nullptr;
    std::string description_;
    std::vector<std::unique_ptr<TaskBase>> tasks_;
    bool requires_db_ = false;
};

} // namespace simdb::pipeline
