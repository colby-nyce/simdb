// <Queue.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <functional>

namespace simdb::pipeline {

/// Base class for all concurrent queues marshalling
/// data between pipeline threads.
class QueueBase
{
public:
    virtual ~QueueBase() = default;
    virtual std::string stringifiedType() const = 0;
    virtual size_t size() const = 0;

private:
    virtual bool hasSnooper_() const = 0;
    virtual SingleQueueSnooperOutcome snoop_(const QueuePrivateIterator&) = 0;
    friend class RunnableFlusher;
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
    void assignQueueItemSnooper_(QueueItemSnooperCallback<T> cb)
    {
        queue_item_snooper_callback_ = cb;
    }

    void assignWholeQueueSnooper_(WholeQueueSnooperCallback<T> cb)
    {
        whole_queue_snooper_callback_ = cb;
    }

    bool hasSnooper_() const override
    {
        return queue_item_snooper_callback_ != nullptr || whole_queue_snooper_callback_ != nullptr;
    }

    SingleQueueSnooperOutcome snoop_(const QueuePrivateIterator& iter) override
    {
        // Give priority to whole-queue snooper if both are set
        if (whole_queue_snooper_callback_)
        {
            return queue_.snoop(iter, whole_queue_snooper_callback_);
        }
        return queue_.snoop(iter, queue_item_snooper_callback_);
    }

    ConcurrentQueue<T> queue_;
    QueueItemSnooperCallback<T> queue_item_snooper_callback_ = nullptr;
    WholeQueueSnooperCallback<T> whole_queue_snooper_callback_ = nullptr;
    friend class RunnableFlusher;
};

template <typename InputType>
using InputQueuePtr = std::unique_ptr<Queue<InputType>>;

template <typename InputType>
inline InputQueuePtr<InputType> makeQueue() { return std::make_unique<Queue<InputType>>(); }

} // namespace simdb::pipeline
