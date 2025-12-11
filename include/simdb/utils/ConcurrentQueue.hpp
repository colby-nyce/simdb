// <ConcurrentQueue.hpp> -*- C++ -*-

#pragma once

#include <mutex>
#include <queue>

namespace simdb {

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
        queue_.push(item);
    }

    /// \brief Push an item to the back of the queue (move version).
    void emplace(T&& item)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.emplace(std::move(item));
    }

    /// \brief Construct an item on the back of the queue.
    ///
    /// \param args Forwarding arguments for the <T> constructor.
    template <typename... Args> void emplace(Args&&... args)
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.emplace(std::forward<Args>(args)...);
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
        queue_.pop();
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

private:
    /// Mutex for thread safety.
    mutable std::mutex mutex_;

    /// FIFO queue to hold the data.
    std::queue<T> queue_;
};

} // namespace simdb
