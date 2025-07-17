// <Task.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/PollingThread.hpp"
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

/// Base class for all terminal tasks.
template <typename InputType>
class TerminalTask : public TaskBase
{
public:
    TerminalTask()
        : input_queue_(makeQueue<InputType>())
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
};

/// Base class for all non-terminal tasks.
template <typename InputType, typename OutputType>
class NonTerminalTask : public TaskBase
{
public:
    NonTerminalTask()
        : input_queue_(makeQueue<InputType>())
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
};

template <typename Element>
class Task;

template <typename Element, typename... Args>
inline std::unique_ptr<Task<Element>> createTask(Args&&... args)
{
    return std::make_unique<Task<Element>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
