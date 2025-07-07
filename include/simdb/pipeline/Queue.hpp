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

private:
    ConcurrentQueue<T> queue_;
};

} // namespace simdb::pipeline
