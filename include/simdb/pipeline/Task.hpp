// <PipelineTask.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Thread.hpp"
#include "simdb/pipeline/Queue.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
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

/// Template the concrete tasks on things like Buffer, Function, Hub, or DatabaseQueue.
/// You can also write your own pipeline element similar to Buffer/Function/etc.
///
/// See test/pipeline/elements/PipelineElements.cpp
/// See simdb/pipeline/elements/*.hpp
///
template <typename Element>
class Task : public TaskBase
{
public:
    using InputType = typename Element::InputType;
    using OutputType = typename Element::OutputType;

    template <typename... Args>
    Task(Args&&... args) : element_(std::forward<Args>(args)...) {}

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
        if (auto q = dynamic_cast<Queue<OutputType>*>(queue))
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
        if (!output_queue_)
        {
            throw DBException("Output queue not set!");
        }        

        InputType in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            ran |= element_(std::move(in), output_queue_->get());
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return element_.getName();
    }

    Queue<InputType> input_queue_;
    Queue<OutputType>* output_queue_ = nullptr;
    Element element_;
};

template <typename Element, typename... Args>
inline std::unique_ptr<Task<Element>> createTask(Args&&... args)
{
    return std::make_unique<Task<Element>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
