// <Queue.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <functional>
#include <memory>

namespace simdb::pipeline {

/*!
 * \class QueueBase
 *
 * \brief Type-erased base for concurrent queues that move data between
 *        pipeline threads. Use Queue<T> for typed access.
 */
class QueueBase
{
public:
    virtual ~QueueBase() = default;
    /// \brief Return a string representation of the queue's value type (e.g. for diagnostics).
    virtual std::string stringifiedType() const = 0;
    /// \brief Return the number of elements currently in the queue.
    virtual size_t size() const = 0;
};

/*!
 * \class Queue
 *
 * \brief Typed wrapper around ConcurrentQueue<T> for pipeline input/output
 *        ports.
 * \tparam T Element type of the queue.
 */
template <typename T> class Queue : public QueueBase
{
public:
    /// \brief Mutable access to the underlying ConcurrentQueue<T>.
    ConcurrentQueue<T>& get() { return queue_; }
    /// \brief Const access to the underlying ConcurrentQueue<T>.
    const ConcurrentQueue<T>& get() const { return queue_; }

    /// \brief Return a string representation of the queue's value type (e.g. for diagnostics).
    std::string stringifiedType() const override { return demangle_type<T>(); }

    /// \brief Return the number of elements currently in the queue.
    size_t size() const override { return queue_.size(); }

private:
    ConcurrentQueue<T> queue_;
};

/// \brief Unique_ptr to a Queue<InputType> (convenience alias).
template <typename InputType> using InputQueuePtr = std::unique_ptr<Queue<InputType>>;

} // namespace simdb::pipeline
