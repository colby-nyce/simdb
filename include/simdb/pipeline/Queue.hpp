// <Queue.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"

namespace simdb::pipeline {

/// Base class for all concurrent queues marshalling
/// data between pipeline threads.
class QueueBase
{
public:
    virtual ~QueueBase() = default;
    virtual std::string stringifiedType() const = 0;
    virtual size_t size() const = 0;
};

/// Wrapper around a concurrent queue which is used by
/// pipeline tasks that know their specific input/output
/// types.
template <typename T>
class Queue : public QueueBase
{
public:
    ConcurrentQueue<T>& get() { return queue_; }
    const ConcurrentQueue<T>& get() const { return queue_; }

    std::string stringifiedType() const override
    {
        return demangle_type<T>();
    }

    size_t size() const override
    {
        return queue_.size();
    }

private:
    ConcurrentQueue<T> queue_;
};

template <typename InputType>
using InputQueuePtr = std::unique_ptr<Queue<InputType>>;

template <typename InputType>
inline InputQueuePtr<InputType> makeQueue() { return std::make_unique<Queue<InputType>>(); }

} // namespace simdb::pipeline
