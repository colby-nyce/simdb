#pragma once

#include "simdb/pipeline/Task.hpp"
#include <functional>

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// Async DB reader task element.
template <typename Input, typename Output>
class AsyncDatabaseReader {};

/// This class is used by pipeline elements to read/update records
/// asynchronously on the DB thread.
template <typename Input, typename Output>
class Task<AsyncDatabaseReader<Input, Output>> : public NonTerminalTask<Input, Output>
{
private:
    using Func = std::function<RunnableOutcome(Input&&, ConcurrentQueue<Output>&, DatabaseManager*, bool)>;

    /// Not meant to be publicly constructible.
    Task(DatabaseManager* db_mgr, Func func)
        : func_(func)
        , db_mgr_(db_mgr)
    {}

    friend class AsyncDatabaseAccessor;

    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), this->output_queue_->get(), db_mgr_, force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        Input in;
        while (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), this->output_queue_->get(), db_mgr_, force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
                break;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    std::string getDescription_() const override
    {
        return "AsyncDatabaseReader<" + demangle_type<Input>() + ", " + demangle_type<Output>() + ">";
    }

    Func func_;
    DatabaseManager* db_mgr_ = nullptr;
};

/// Specialization for terminal DB reads/updates.
template <typename Input>
class Task<AsyncDatabaseReader<Input, void>> : public TerminalTask<Input>
{
private:
    using Func = std::function<RunnableOutcome(Input&&, DatabaseManager*, bool)>;

    /// Not meant to be publicly constructible.
    Task(DatabaseManager* db_mgr, Func func)
        : func_(func)
        , db_mgr_(db_mgr)
    {}

    friend class AsyncDatabaseAccessor;

    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), db_mgr_, force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        Input in;
        while (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), db_mgr_, force);
            if (o == RunnableOutcome::ABORT_FLUSH && !force)
            {
                throw DBException("Cannot issue ABORT_FLUSH when we are not flushing!");
            }
            else if (o == RunnableOutcome::ABORT_FLUSH)
            {
                outcome = RunnableOutcome::ABORT_FLUSH;
                break;
            }
            else if (o == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
        }
        return outcome;
    }

    std::string getDescription_() const override
    {
        return "AsyncDatabaseReader<" + demangle_type<Input>() + ", void>";
    }

    Func func_;
    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb::pipeline
