#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"

namespace simdb::pipeline {

class QueueBase
{
public:
    virtual ~QueueBase() = default;
};

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
