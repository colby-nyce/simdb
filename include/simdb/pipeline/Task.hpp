// <Task.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Thread.hpp"
#include "simdb/pipeline/Queue.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <memory>

namespace simdb::pipeline {

/// Base class for all pipeline tasks.
class TaskBase : public Runnable
{
public:
    virtual QueueBase* getInputQueue() = 0;
    virtual void setOutputQueue(QueueBase* q) = 0;
    virtual bool requiresDatabase() const = 0;
    virtual void setDatabaseManager(DatabaseManager*) = 0;

    template <typename Input>
    ConcurrentQueue<Input>* getTypedInputQueue(bool expect_valid = true)
    {
        if (!getInputQueue() && expect_valid)
        {
            throw DBException("No input queue");
        }

        if (auto q = dynamic_cast<Queue<Input>*>(getInputQueue()))
        {
            return &q->get();
        }
        else if (expect_valid)
        {
            throw DBException("Invalid data type");
        }

        return nullptr;
    }

    TaskBase& operator>>(TaskBase& next)
    {
        setOutputQueue(next.getInputQueue());
        return next;
    }
};

/// Base class for all terminal non-database tasks.
template <typename InputType>
class TerminalNonDatabaseTask : public TaskBase
{
public:
    TerminalNonDatabaseTask(InputQueuePtr<InputType> input_queue = nullptr)
        : input_queue_(input_queue ? std::move(input_queue) : makeQueue<InputType>())
    {}

    QueueBase* getInputQueue() override final
    {
        return this->input_queue_.get();
    }

protected:
    InputQueuePtr<InputType> input_queue_;

private:
    void setOutputQueue(QueueBase*) override final
    {
        throw DBException("Cannot set output queue on terminal task");
    }

    bool requiresDatabase() const override final
    {
        return false;
    }

    void setDatabaseManager(DatabaseManager*) override final
    {
        throw DBException("Cannot give database to a non-database task");
    }
};

/// Base class for all non-terminal, non-database tasks.
template <typename InputType, typename OutputType>
class NonTerminalNonDatabaseTask : public TaskBase
{
public:
    NonTerminalNonDatabaseTask(InputQueuePtr<InputType> input_queue = nullptr)
        : input_queue_(input_queue ? std::move(input_queue) : makeQueue<InputType>())
    {}

    QueueBase* getInputQueue() override final
    {
        return this->input_queue_.get();
    }

    void setOutputQueue(QueueBase* queue) override final
    {
        if (auto q = dynamic_cast<Queue<OutputType>*>(queue))
        {
            this->output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

protected:
    InputQueuePtr<InputType> input_queue_;
    Queue<OutputType>* output_queue_ = nullptr;

private:
    bool requiresDatabase() const override final
    {
        return false;
    }

    void setDatabaseManager(DatabaseManager*) override final
    {
        throw DBException("Cannot give database to a non-database task");
    }

};

/// Base class for all terminal database tasks.
template <typename InputType>
class TerminalDatabaseTask : public TaskBase
{
public:
    TerminalDatabaseTask(InputQueuePtr<InputType> input_queue = nullptr)
        : input_queue_(input_queue ? std::move(input_queue) : makeQueue<InputType>())
    {}

    QueueBase* getInputQueue() override final
    {
        return this->input_queue_.get();
    }

protected:
    DatabaseManager* getDatabaseManager_() const
    {
        return db_mgr_;
    }

    InputQueuePtr<InputType> input_queue_;

private:
    void setOutputQueue(QueueBase*) override final
    {
        throw DBException("Cannot set output queue on terminal task");
    }

    bool requiresDatabase() const override final
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override final
    {
        db_mgr_ = db_mgr;
    }

    DatabaseManager* db_mgr_ = nullptr;
};

/// Base class for all non-terminal database tasks.
template <typename InputType, typename OutputType>
class NonTerminalDatabaseTask : public TaskBase
{
public:
    NonTerminalDatabaseTask(InputQueuePtr<InputType> input_queue = nullptr)
        : input_queue_(input_queue ? std::move(input_queue) : makeQueue<InputType>())
    {}

    QueueBase* getInputQueue() override final
    {
        return this->input_queue_.get();
    }

    void setOutputQueue(QueueBase* queue) override final
    {
        if (auto q = dynamic_cast<Queue<OutputType>*>(queue))
        {
            this->output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

protected:
    DatabaseManager* getDatabaseManager_() const
    {
        return db_mgr_;
    }

    InputQueuePtr<InputType> input_queue_;
    Queue<OutputType>* output_queue_ = nullptr;

private:
    bool requiresDatabase() const override final
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override final
    {
        db_mgr_ = db_mgr;
    }

    DatabaseManager* db_mgr_ = nullptr;
};

template <typename Element>
class Task;

/// Template the concrete tasks on things like Buffer, Function, Hub,
/// or DatabaseQueue. You can also create your own pipeline element by
/// specializing the Task class similar to Function.hpp, Buffer.hpp,
/// or test/pipeline/elements/main.cpp (CircularBuffer).
///
template <typename Element, typename... Args>
inline std::unique_ptr<Task<Element>> createTask(Args&&... args)
{
    return std::make_unique<Task<Element>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
