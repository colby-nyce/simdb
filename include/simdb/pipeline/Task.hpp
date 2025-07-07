// <Task.hpp> -*- C++ -*-

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

template <typename Element>
class Task;

/// Template the concrete tasks on things like Buffer, Function, Hub,
/// or DatabaseQueue. You can also create your own pipeline element by
/// specializing the Task class similar to Function.hpp, Buffer.hpp,
/// or test/pipeline/elements/CircularBuffer.hpp
///
template <typename Element, typename... Args>
inline std::unique_ptr<Task<Element>> createTask(Args&&... args)
{
    return std::make_unique<Task<Element>>(std::forward<Args>(args)...);
}

} // namespace simdb::pipeline
