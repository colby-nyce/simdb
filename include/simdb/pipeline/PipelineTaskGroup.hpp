#pragma once

#include "simdb/pipeline/PipelineTask.hpp"

namespace simdb::pipeline {

class TaskGroup : public Runnable
{
public:
    TaskGroup(const std::string& pipeline_name, const std::string& description = "")
        : pipeline_name_(pipeline_name)
        , description_(description)
    {}

    void addTask(std::unique_ptr<TaskBase> task, const std::string& description = "")
    {
        std::string task_name = pipeline_name_ + "." + task->getName();
        if (!description.empty())
        {
            task_name += " (" + description + ")";
        }
        task->setName(task_name);

        if (!tasks_.empty())
        {
            auto prev = tasks_.back().get();
            prev->setOutputQueue(task->getInputQueue());
        }
        requires_db_ |= dynamic_cast<const DatabaseTask*>(task.get()) != nullptr;
        tasks_.emplace_back(std::move(task));
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
            if (auto db_task = dynamic_cast<DatabaseTask*>(task.get()))
            {
                db_task->setDatabaseManager(db_mgr);
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
        if (auto q = dynamic_cast<PipelineQueue<Input>*>(queue))
        {
            return &q->get();
        }
        return nullptr;
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
    std::string description_;
    std::vector<std::unique_ptr<TaskBase>> tasks_;
    bool requires_db_ = false;
};

} // namespace simdb::pipeline
