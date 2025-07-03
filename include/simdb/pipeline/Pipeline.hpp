#pragma once

#include "simdb/pipeline/PipelineTask.hpp"
#include <set>

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

    template <typename TaskT = TaskBase>
    std::vector<TaskT*> getTasks()
    {
        static_assert(std::is_base_of<TaskBase, TaskT>::value);

        std::vector<TaskT*> tasks;
        for (auto& task : tasks_)
        {
            if constexpr (std::is_same<TaskT, TaskBase>::value)
            {
                tasks.emplace_back(task.get());
            }
            else if (auto t = dynamic_cast<TaskT*>(task.get()))
            {
                tasks.emplace_back(t);
            }
        }
        return tasks;
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

    bool requiresDatabase() const
    {
        return requires_db_;
    }

private:
    DatabaseManager* db_mgr_ = nullptr;
    std::string pipeline_name_;
    std::vector<std::unique_ptr<TaskBase>> tasks_;
    bool requires_db_ = false;
};

} // namespace simdb::pipeline
