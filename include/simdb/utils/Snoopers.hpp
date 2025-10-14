// <Snoopers.hpp> -*- C++ -*-

#pragma once

#include <deque>
#include <functional>

/// Utilities which support task queue "snoopers". A snooper is a
/// callback function that can be assigned to a task's input
/// queue in order to "peek" at items in the queue without
/// actually popping them off the queue. This is useful for
/// implementing short-circuiting of a pipeline flush operation
/// when a specific item is seen in the queue.

namespace simdb {

enum class SnooperCallbackOutcome
{
    // The snooper found what it was looking for, and wants
    // to stop further snooping.
    FOUND_STOP,

    // The snooper found what it was looking for, but wants
    // to continue snooping other queues.
    FOUND_CONTINUE,

    // The snooper did not find what it was looking for, but
    // wants to stop further snooping.
    NOT_FOUND_STOP,

    // The snooper did not find what it was looking for, and
    // wants to continue snooping other queues.
    NOT_FOUND_CONTINUE
};

/// Callback for per-item queue snooping.
template <typename T>
using QueueItemSnooperCallback = std::function<SnooperCallbackOutcome(const T& queue_item)>;

/// Callback for whole-queue snooping.
template <typename T>
using WholeQueueSnooperCallback = std::function<SnooperCallbackOutcome(std::deque<T>& queue)>;

/// Outcome of a single queue snoop operation.
struct SingleQueueSnooperOutcome
{
    bool found = false;
    bool done = false;
    uint32_t num_items_peeked = 0;
};

/// Outcome of a RunnableFlusher snoop operation.
struct RunnableFlusherSnooperOutcome
{
    bool found = false;
    uint32_t num_queues_peeked = 0;
    uint32_t num_items_peeked = 0;
};

} // namespace simdb
