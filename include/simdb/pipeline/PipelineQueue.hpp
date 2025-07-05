// <PipelineQueue.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"

namespace simdb::pipeline {

/// Base class for all concurrent queues marshalling
/// data between pipeline threads.
class QueueBase
{
public:
    virtual ~QueueBase() = default;
};

/// Wrapper around a concurrent queue which is used by
/// pipeline tasks that know their specific input/output
/// types.
template <typename T>
class PipelineQueue : public QueueBase
{
public:
    ConcurrentQueue<T>& get() { return queue_; }
    const ConcurrentQueue<T>& get() const { return queue_; }

private:
    ConcurrentQueue<T> queue_;
};

} // namespace simdb::pipeline
