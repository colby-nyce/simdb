// <Runnable.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/Exceptions.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace simdb::pipeline {

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
    virtual bool processOne(bool force) = 0;

    /// Flush and process everything from the input queue,
    /// returning true if this runnable did anything.
    virtual bool processAll(bool force) = 0;

    /// Print info about this runnable for reporting purposes.
    virtual void print(std::ostream& os, int indent) const
    {
        os << std::string(indent, ' ') << getDescription() << "\n";
    }

private:
    virtual std::string getDescription_() const = 0;
    std::string description_;
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
    }

    /// Call processAll() on all runnables in a single transaction.
    /// This will flush the leftmost runnable first, then the next, etc.
    ///
    ///    Task1    Task2    Task3
    ///    a-b-c-d--e-f-g-h--i-j-k-l
    ///
    /// Flush: a, b, c, d, e, f, g, h, i, j, k, l
    void waterfallFlush()
    {
        db_mgr_.safeTransaction(
            [&]()
            {
                bool continue_while;
                do
                {
                    continue_while = false;
                    for (Runnable* r : runnables_)
                    {
                        try
                        {
                            continue_while |= r->processAll(true);
                        }
                        catch(const FlushCancelledException&)
                        {
                            continue_while = false;
                            break;
                        }
                    }
                } while (continue_while);
            });
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
    void roundRobinFlush()
    {
        db_mgr_.safeTransaction(
            [&]()
            {
                bool continue_while;
                do
                {
                    continue_while = false;
                    for (Runnable* r : runnables_)
                    {
                        try
                        {
                            continue_while |= r->processOne(true);
                        }
                        catch(const FlushCancelledException&)
                        {
                            continue_while = false;
                            break;
                        }
                    }
                } while (continue_while);
            });
    }

private:
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

    DatabaseManager& db_mgr_;
    std::vector<Runnable*> runnables_;
};

} // namespace simdb::pipeline
