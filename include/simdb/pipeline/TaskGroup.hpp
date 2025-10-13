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
        task->setTaskGroup_(this);
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
    RunnableOutcome processOne(bool force) override
    {
        return process_(true, force);
    }

    RunnableOutcome processAll(bool force) override
    {
        return process_(false, force);
    }

    RunnableOutcome process_(bool one, bool force)
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        for (auto& task : tasks_)
        {
            if (!task->enabled())
            {
                continue;
            }

            auto o = one ? task->processOne(force) : task->processAll(force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
                break;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
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

    void setPollingThread_(PollingThread* pt) override
    {
        Runnable::setPollingThread_(pt);
        for (auto& task : tasks_)
        {
            task->setPollingThread_(pt);
        }
    }

    std::string pipeline_name_;
    std::string description_;
    std::vector<std::unique_ptr<TaskBase>> tasks_;
};

/// Defined here so we can avoid circular includes
inline void RunnableFlusher::determineDisablerRunnables_()
{
    if (!disabler_runnables_.empty())
    {
        return;
    }

    std::map<TaskGroup*, std::vector<Runnable*>> tg_map;
    for (auto r : runnables_)
    {
        if (auto tg = r->getTaskGroup_())
        {
            tg_map[tg].push_back(r);
        }
    }

    for (auto& [tg, rs] : tg_map)
    {
        if (rs.size() == tg->getTasks().size())
        {
            // All tasks in this TaskGroup are part of the flusher,
            // so disable the entire TaskGroup instead of individual tasks.
            disabler_runnables_.push_back(tg);
        }
        else
        {
            // Only some tasks in this TaskGroup are part of the flusher,
            // so disable individual tasks instead of the entire TaskGroup.
            disabler_runnables_.insert(disabler_runnables_.end(), rs.begin(), rs.end());
        }
    }

    // Add any runnables that are not part of a TaskGroup
    for (auto r : runnables_)
    {
        if (!r->getTaskGroup_())
        {
            disabler_runnables_.push_back(r);
        }
    }

    // Ensure no duplicates
    auto end = std::unique(disabler_runnables_.begin(), disabler_runnables_.end());
    disabler_runnables_.erase(end, disabler_runnables_.end());

    // It is more efficient to disable/enable runnables in reverse order
    //std::reverse(disabler_runnables_.begin(), disabler_runnables_.end());
}

} // namespace simdb::pipeline
