#pragma once

#include "simdb/pipeline/PipelineThread.hpp"
#include "simdb/pipeline/PipelineQueue.hpp"
#include "simdb/pipeline/DatabaseQueue.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include <memory>

namespace simdb::pipeline {

class QueueBase;

class TaskBase : public Runnable
{
public:
    virtual QueueBase* getInputQueue() = 0;
    virtual QueueBase* getOutputQueue() = 0;
    virtual void setOutputQueue(QueueBase* q) = 0;

protected:
    TaskBase(const std::string& name)
        : Runnable(name)
    {}
};

class DatabaseTask : public TaskBase
{
public:
    DatabaseTask(const std::string& name) : TaskBase(name) {}
    void setDatabaseManager(DatabaseManager* db_mgr) { db_mgr_ = db_mgr; }
    DatabaseManager* getDatabaseManager() const { return db_mgr_; }

private:
    DatabaseManager* db_mgr_ = nullptr;
};

template <typename TaskIn, typename TaskOut>
class Task : public TaskBase
{
public:
    using Func = std::function<void(TaskIn&&, ConcurrentQueue<TaskOut>&)>;

    Task(const std::string& name, Func func)
        : TaskBase(name)
        , func_(func)
    {}

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return output_queue_;
    }

    void setOutputQueue(QueueBase* q) override
    {
        if (auto queue = dynamic_cast<PipelineQueue<TaskOut>*>(q))
        {
            output_queue_ = queue;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    bool run() override
    {
        if (!output_queue_)
        {
            throw DBException("Output queue not set");
        }

        TaskIn data;
        bool ran = false;
        while (input_queue_.get().try_pop(data))
        {
            func_(std::move(data), output_queue_->get());
            ran = true;
        }
        return ran;
    }

private:
    Func func_;
    PipelineQueue<TaskIn> input_queue_;
    PipelineQueue<TaskOut>* output_queue_ = nullptr;
};

template <typename DatabaseIn>
class Task<DatabaseQueue<DatabaseIn>, void> : public DatabaseTask
{
public:
    using DatabaseFunc = std::function<void(DatabaseIn&&, DatabaseManager*)>;

    Task(DatabaseFunc db_func)
        : DatabaseTask("DatabaseQueue<" + demangle_type<DatabaseIn>() + ">")
        , db_func_(db_func)
    {}

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return nullptr;
    }

    void setOutputQueue(QueueBase*) override
    {
        throw DBException("Cannot set downstream tasks - must terminate at database");
    }

    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            db_func_(std::move(in), getDatabaseManager());
            ran = true;
        }
        return ran;
    }

private:
    DatabaseFunc db_func_;
    PipelineQueue<DatabaseIn> input_queue_;
};

template <typename TaskIn>
class Task<TaskIn, void> : public TaskBase
{
public:
    using Func = std::function<void(TaskIn&&)>;

    Task(const std::string& name, Func func)
        : TaskBase(name)
        , func_(func)
    {}

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return nullptr;
    }

    void setOutputQueue(QueueBase*) override
    {
        throw DBException("Cannot have downstream tasks: Task<T,void>");
    }

    bool run() override
    {
        TaskIn in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            func_(std::move(in));
            ran = true;
        }
        return ran;
    }

private:
    Func func_;
    PipelineQueue<TaskIn> input_queue_;
};

template <typename TaskIn, typename TaskOut, typename... Args>
inline std::unique_ptr<Task<TaskIn,TaskOut>> createTask(Args&&... args)
{
    return std::make_unique<Task<TaskIn,TaskOut>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
