// <Runnable.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Snoopers.hpp"
#include "simdb/Exceptions.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace simdb::pipeline {

class TaskBase;

/// Various outcomes for each processOne/processAll calls to a runnable:
enum class RunnableOutcome
{
    // Return if the runnable (task) pushed any data to its output queue,
    // or otherwise should leave the pipeline tasks greedily executing
    // as normal.
    DID_WORK,

    // Return if the runnable had no data to consume or otherwise should
    // tell the pipeline thread to go back to sleep for a bit.
    NO_OP,

    // Return if the runnable should explicitly stop a RunnableFlusher
    // from continuing to flush the whole pipeline.
    ABORT_FLUSH
};

/// Base class for all things that can be run on a pipeline thread.
class Runnable
{
public:
    virtual ~Runnable() = default;

    /// Get this runnable's description.
    std::string getDescription() const
    {
        return !description_.empty() ? description_ : getDescription_();
    }

    /// Set/overwrite the this runnable's description.
    void setDescription(const std::string& desc)
    {
        description_ = desc;
    }

    /// Process one item from the input queue, returning true
    /// if this runnable did anything.
    virtual RunnableOutcome processOne(bool force) = 0;

    /// Flush and process everything from the input queue,
    /// returning true if this runnable did anything.
    virtual RunnableOutcome processAll(bool force) = 0;

    /// Print info about this runnable for reporting purposes.
    virtual void print(std::ostream& os, int indent) const
    {
        os << std::string(indent, ' ') << getDescription() << "\n";
    }

    /// Check if this runnable is enabled.
    bool enabled() const
    {
        return enabled_;
    }

    /// Disable/re-enable this runnable. Subclasses that do not
    /// allow this control override and throw.
    virtual void enable(bool enable = true)
    {
        enabled_ = enable;
    }

private:
    virtual std::string getDescription_() const = 0;
    std::string description_;
    bool enabled_ = true;
};

/// RAII utility used to disable all runnables in a RunnableFlusher
/// while in scope, re-enabling them when going out of scope.
class ScopedRunnableDisabler
{
public:
    ~ScopedRunnableDisabler()
    {
        for (auto r : disabled_)
        {
            r->enable(true);
        }
    }

private:
    ScopedRunnableDisabler(const std::vector<Runnable*>& runnables)
    {
        for (auto r : runnables)
        {
            if (r->enabled())
            {
                r->enable(false);
                disabled_.push_back(r);
            }
        }
    }

    std::vector<Runnable*> disabled_;
    friend class RunnableFlusher;
};

/// This class takes a variable number of Runnable pointers (raw or smart)
/// and flushes them on demand using a "waterfall" or "round robin" algo.
class RunnableFlusher
{
public:
    /// Constructor taking any number of Runnable pointers (raw or smart).
    /// Pass in runnables in the order you want them flushed. Must pass in
    /// the app's DatabaseManager so all work can be done in a transaction.
    template <typename... Args>
    RunnableFlusher(DatabaseManager& db_mgr, Args&&... args)
        : db_mgr_(db_mgr)
    {
        addRunnables_(std::forward<Args>(args)...);
        addTasks_();
    }

    /// Assign a snooper callback to a task's input queue. The task
    /// must be part of this RunnableFlusher, and the input queue
    /// must be of the correct type T.
    template <typename T>
    void assignSnooper(TaskBase& t, const SnooperCallback<T>& cb);

    /// Snoop all tasks that have snoopers assigned, returning
    /// an outcome that indicates if any snooper found what it
    /// was looking for.
    SnooperOutcome snoopAll();

    /// Use this API to temporarily disable all runnables while snooping.
    /// This is useful if you want to ensure that no data is processed
    /// or augmented or destroyed while snooping the pipeline task queues.
    /// The use case for doing so is if you want to use data directly from
    /// the queues instead of copying/cloning for better performance.
    ScopedRunnableDisabler scopedDisableAll()
    {
        return ScopedRunnableDisabler(runnables_);
    }

    /// Call processAll() on all runnables in a single transaction.
    /// This will flush the leftmost runnable first, then the next, etc.
    ///
    ///    Task1    Task2    Task3
    ///    a-b-c-d--e-f-g-h--i-j-k-l
    ///
    /// Flush: a, b, c, d, e, f, g, h, i, j, k, l
    ///
    /// Returns true if there was anything to flush at all.
    bool waterfallFlush()
    {
        return process_(false, SIZE_MAX);
    }

    /// Call processOne() on all runnables in a single transaction.
    //
    /// This will process one item from the leftmost runnable first, then
    /// one from the next, and so on in a round robin fashion until all
    /// runnables are empty.
    ///
    ///    Task1    Task2    Task3
    ///    a-b-c-d--e-f-g-h--i-j-k-l
    ///
    /// Flush: a, e, i, b, f, j, c, g, k, d, h, l
    ///
    /// Optionally pass in the max number of round robins we should perform.
    ///
    /// Returns true if there was anything to flush at all.
    bool roundRobinFlush(size_t max_round_robins = SIZE_MAX)
    {
        return process_(true, max_round_robins);
    }

private:
    bool process_(bool one, size_t max_loops)
    {
        // Max loops of zero does not make sense.
        if (max_loops == 0)
        {
            max_loops = SIZE_MAX;
        }

        bool did_anything = false;

        db_mgr_.safeTransaction(
            [&]()
            {
                bool continue_while;
                size_t num_loops = 0;
                do
                {
                    continue_while = false;
                    for (Runnable* r : runnables_)
                    {
                        if (!r->enabled())
                        {
                            continue;
                        }

                        auto outcome = one ? r->processOne(true) : r->processAll(true);
                        if (outcome == RunnableOutcome::ABORT_FLUSH)
                        {
                            continue_while = false;
                            break;
                        }
                        else if (outcome == RunnableOutcome::DID_WORK)
                        {
                            continue_while = true;
                            did_anything = true;
                        }
                    }

                    if (++num_loops == max_loops)
                    {
                        continue_while = false;
                    }
                } while (continue_while);
            });

        return did_anything;
    }

    template <typename T>
    Runnable* unwrap_(T* ptr) const
    {
        static_assert(std::is_base_of<Runnable, T>::value,
                      "All arguments must be or point to Runnable");
        return ptr;
    }

    template <typename T>
    Runnable* unwrap_(const std::shared_ptr<T>& ptr) const
    {
        static_assert(std::is_base_of<Runnable, T>::value,
                      "All arguments must be or point to Runnable");
        return ptr.get();
    }

    template <typename T>
    Runnable* unwrap_(const std::unique_ptr<T>& ptr) const
    {
        static_assert(std::is_base_of<Runnable, T>::value,
                      "All arguments must be or point to Runnable");
        return ptr.get();
    }

    // Recursive variadic unpacking
    template <typename First, typename... Rest>
    void addRunnables_(First&& first, Rest&&... rest)
    {
        Runnable* r = unwrap_(std::forward<First>(first));
        if (!r) {
            throw std::invalid_argument("Null Runnable pointer passed to RunnableFlusher");
        }
        runnables_.push_back(r);
        addRunnables_(std::forward<Rest>(rest)...);
    }

    // Base case
    void addRunnables_()
    {
    }

    // Store the TaskBase* for faster snooping without dynamic_cast
    void addTasks_();

    DatabaseManager& db_mgr_;
    std::vector<Runnable*> runnables_;
    std::vector<TaskBase*> tasks_;
};

} // namespace simdb::pipeline
