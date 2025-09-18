// <Runnable.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"

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

    /// Flush and process everything. Return true if the runnable
    /// actually did anything, false if there was no input data.
    virtual bool run(bool force_flush) = 0;

    /// Perform end-of-simulation flush to pipeline. The pipeline
    /// threads have been stopped, so you don't have to worry about
    /// thread safety of your subclass objects.
    virtual bool flushToPipeline()
    {
        bool did_work = false;
        while (run(true))
        {
            did_work = true;
        }
        return did_work;
    }

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
/// and calls flushToPipeline() on each of them when flush() is called.
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

    /// Call flushToPipeline() on all runnables in a single transaction.
    void flush()
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
                        continue_while |= r->flushToPipeline();
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
