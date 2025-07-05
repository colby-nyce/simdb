#pragma once

#include "simdb/pipeline/PipelineThread.hpp"
#include "simdb/pipeline/PipelineQueue.hpp"
#include "simdb/pipeline/DatabaseQueue.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include <memory>

namespace simdb::pipeline {

class QueueBase;

/// Base class for all pipeline tasks.
class TaskBase : public Runnable
{
public:
    virtual QueueBase* getInputQueue() = 0;
    virtual QueueBase* getOutputQueue() = 0;
    virtual void setOutputQueue(QueueBase* q) = 0;
    virtual bool requiresDatabase() const = 0;
    virtual void setDatabaseManager(DatabaseManager*) {}
};

/// Unspecialized template class to operate on a <TaskIn> data type
/// and output a <TaskOut> data type.
template <typename TaskIn, typename TaskOut>
class Task : public TaskBase
{
public:
    using Func = std::function<void(TaskIn&&, ConcurrentQueue<TaskOut>&)>;

    Task(Func func)
        : func_(func)
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

    bool requiresDatabase() const override
    {
        return false;
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
    std::string getName_() const override
    {
        return "Task<" + demangle_type<TaskIn>() + ", " + demangle_type<TaskOut>() + ">";
    }

    Func func_;
    PipelineQueue<TaskIn> input_queue_;
    PipelineQueue<TaskOut>* output_queue_ = nullptr;
};

/// Specialization for pipeline tasks that terminate at the database.
template <typename DatabaseIn>
class Task<DatabaseQueue<DatabaseIn>, void> : public TaskBase
{
public:
    using StdFunc = std::function<void(DatabaseIn&&, DatabaseManager*)>;
    Task(DatabaseManager* db_mgr, StdFunc func) : db_mgr_(db_mgr), db_func_(func) {}

    using FreeFunc = void (*)(DatabaseIn&&, DatabaseManager*);
    Task(DatabaseManager* db_mgr, FreeFunc func) : db_mgr_(db_mgr)
    {
        db_func_ = [free_func = func](DatabaseIn&& in, DatabaseManager* db_mgr)
        {
            free_func(std::move(in), db_mgr);
        };
    }

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

    bool requiresDatabase() const override
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override
    {
        db_mgr_ = db_mgr;
    }

    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            db_func_(std::move(in), db_mgr_);
            ran = true;
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", void>";
    }

    DatabaseManager* db_mgr_ = nullptr;
    StdFunc db_func_;
    PipelineQueue<DatabaseIn> input_queue_;
};

template <typename Input>
class Buffer {};

/// Pipeline task specialization. Implements a simple buffer that accepts
/// data one at a time, and will only output a vector of that data when
/// the buffer is full.
template <typename DataT>
class Task<DataT, Buffer<DataT>> : public TaskBase
{
public:
    Task(size_t buffer_len) : buffer_len_(buffer_len) {}

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return output_queue_;
    }

    void setOutputQueue(QueueBase* queue) override
    {
        if (auto q = dynamic_cast<PipelineQueue<std::vector<DataT>>*>(queue))
        {
            output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    bool requiresDatabase() const override
    {
        return false;
    }

    bool run() override
    {
        DataT in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            buffer_.emplace_back(std::move(in));
            if (buffer_.size() == buffer_len_)
            {
                output_queue_->get().emplace(std::move(buffer_));
                assert(buffer_.empty());
                ran = true;
            }
        }

        return ran;
    }

private:
    std::string getName_() const override
    {
        return "Buffer<" + demangle_type<DataT>() + ">";
    }

    size_t buffer_len_ = 0;
    PipelineQueue<DataT> input_queue_;
    PipelineQueue<std::vector<DataT>>* output_queue_ = nullptr;
    std::vector<DataT> buffer_;
};

template <typename FunctionOut>
class Function {};

/// Pipeline task specialization. Implements a simple function call
/// which can be either a std::function or a free function with the
/// given I/O data types.
template <typename FunctionIn, typename FunctionOut>
class Task<FunctionIn, Function<FunctionOut>> : public TaskBase
{
public:
    using StdFunc = std::function<FunctionOut(FunctionIn&&)>;
    Task(StdFunc func) : func_(func) {}

    using FreeFunc = FunctionOut&& (*)(FunctionIn&&);
    Task(FreeFunc func)
    {
        func_ = [free_func = func](FunctionIn&& in)
        {
            return std::move(free_func(std::move(in)));
        };
    }

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return output_queue_;
    }

    void setOutputQueue(QueueBase* queue) override
    {
        if (auto q = dynamic_cast<PipelineQueue<FunctionOut>*>(queue))
        {
            output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    bool requiresDatabase() const override
    {
        return false;
    }

    bool run() override
    {
        FunctionIn in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            FunctionOut out = func_(std::move(in));
            output_queue_->get().emplace(std::move(out));
            ran = true;
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return "Function<" + demangle_type<FunctionIn>() + ", " + demangle_type<FunctionOut>() + ">";
    }

    StdFunc func_;
    PipelineQueue<FunctionIn> input_queue_;
    PipelineQueue<FunctionOut>* output_queue_ = nullptr;
};

template <typename TaskIn, typename TaskOut, typename... Args>
inline std::unique_ptr<Task<TaskIn,TaskOut>> createTask(Args&&... args)
{
    return std::make_unique<Task<TaskIn,TaskOut>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
