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

/// Defined here so we can avoid circular includes
template <typename T>
inline void RunnableFlusher::assignSnooper(TaskBase& t, const SnooperCallback<T>& cb)
{
    auto it = std::find(tasks_.begin(), tasks_.end(), &t);
    if (it == tasks_.end())
    {
        throw DBException("Task is not part of this RunnableFlusher");
    }

    auto q = t.getInputQueue();
    if (!q)
    {
        throw DBException("Task has no input queue");
    }

    auto q_typed = dynamic_cast<Queue<T>*>(q);
    if (!q_typed)
    {
        throw DBException("Task input queue is not of the correct type");
    }

    q_typed->assignSnooper_(cb);
}

/// Defined here so we can avoid circular includes
inline SnooperOutcome RunnableFlusher::snoopAll()
{
    SnooperOutcome outcome;

    for (auto t : tasks_)
    {
        auto q = t->getInputQueue();

        if (q && q->hasSnooper_())
        {
            auto task_snooper_outcome = q->snoop_(QueuePrivateIterator{});
            outcome.num_items_peeked += task_snooper_outcome.num_items_peeked;
            outcome.num_queues_peeked += 1;

            if (task_snooper_outcome.found)
            {
                outcome.found = true;
            }

            if (task_snooper_outcome.done)
            {
                break;
            }
        }
    }

    return outcome;
}

/// Defined here so we can avoid circular includes
inline void RunnableFlusher::addTasks_()
{
    for (auto r : runnables_)
    {
        if (auto t = dynamic_cast<TaskBase*>(r))
        {
            tasks_.push_back(t);
        }
    }
}

/// Defined here so we can avoid circular includes
inline void RunnableFlusher::addPollingThreads_()
{
    if (!polling_threads_.empty())
    {
        return;
    }

    for (auto t : tasks_)
    {
        if (auto pt = t->getPollingThread_())
        {
            // Avoid duplicates
            if (std::find(polling_threads_.begin(), polling_threads_.end(), pt) == polling_threads_.end())
            {
                polling_threads_.push_back(pt);
            }
        }
    }
}

template <typename Element>
class Task;

template <typename Element, typename... Args>
inline std::unique_ptr<Task<Element>> createTask(Args&&... args)
{
    return std::make_unique<Task<Element>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
