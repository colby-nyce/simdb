// <ConcurrentQueue.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/Snoopers.hpp"

#include <mutex>
#include <deque>

namespace simdb {

namespace pipeline {
    class RunnableFlusher;
    class QueuePrivateIterator
    {
    private:
        QueuePrivateIterator() = default;
        friend class RunnableFlusher;
    };
} // namespace simdb::pipeline

/*! 
 * \class ConcurrentQueue<T>
 *
 * \brief Thread-safe wrapper around std::queue
 */
template <typename T>
class ConcurrentQueue
{
public:
    /// \brief Push an item to the back of the queue.
    void push(const T& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.push_back(item);
    }

    /// \brief Push an item to the back of the queue (move version).
    void emplace(T&& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.emplace_back(std::move(item));
    }

    /// \brief Construct an item on the back of the queue.
    ///
    /// \param args Forwarding arguments for the <T> constructor.
    template <typename... Args> void emplace(Args&&... args)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.emplace_back(std::forward<Args>(args)...);
    }

    /// \brief Get the item at the front of the queue.
    ///
    /// \param item Output argument for the popped item.
    ///
    /// \return Returns true if successful, or false if there
    ///         was no data in the queue.
    bool try_pop(T& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (queue_.empty())
        {
            return false;
        }
        std::swap(item, queue_.front());
        queue_.pop_front();
        return true;
    }

    /// \brief Get the number of items in this queue.
    size_t size() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.size();
    }

    /// \brief Check for empty
    bool empty() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return queue_.empty();
    }

    /// \brief Snoop all items in the queue without popping them.
    QueueSnooperOutcome snoop(const pipeline::QueuePrivateIterator&, const SnooperCallback<T>& cb)
    {
        std::lock_guard<std::mutex> guard(mutex_);

        QueueSnooperOutcome outcome;
        for (const auto& item : queue_)
        {
            outcome.num_items_peeked++;
            auto cb_outcome = cb(item);
            switch (cb_outcome)
            {
                case QueueItemSnooperOutcome::FOUND_STOP:
                    outcome.found = true;
                    outcome.done = true;
                    return outcome;

                case QueueItemSnooperOutcome::FOUND_CONTINUE:
                    outcome.found = true;
                    break;

                case QueueItemSnooperOutcome::NOT_FOUND_STOP:
                    outcome.done = true;
                    return outcome;

                case QueueItemSnooperOutcome::NOT_FOUND_CONTINUE:
                    break;
            }
        }
        return outcome;
    }

private:
    /// Mutex for thread safety.
    mutable std::mutex mutex_;

    /// FIFO queue. We use a deque to support snooping.
    std::deque<T> queue_;
};

} // namespace simdb
