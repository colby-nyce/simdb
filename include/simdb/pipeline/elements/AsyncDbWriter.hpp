#pragma once

#include "simdb/pipeline/Task.hpp"
#include <functional>

namespace simdb {
    class DatabaseManager;
    class PreparedINSERT;
}

namespace simdb::pipeline {

/// This class is given to AsyncDatabaseWriter task functions
/// so users can write to their app's tables using prepared
/// statements, which is typically much faster than creating
/// a new statement every time (DatabaseManager::INSERT).
class AppPreparedINSERTs
{
public:
    /// \note PreparedINSERT is defined in DatabaseManager.hpp
    using TableInserters = std::unordered_map<std::string, std::unique_ptr<PreparedINSERT>>;

    AppPreparedINSERTs(TableInserters&& tbl_inserters)
        : tbl_inserters_(std::move(tbl_inserters))
    {
    }

    PreparedINSERT* getPreparedINSERT(const std::string& table_name) const
    {
        return tbl_inserters_.at(table_name).get();
    }

private:
    TableInserters tbl_inserters_;
};

/// Async DB writer task element.
template <typename App, typename Input, typename Output>
class AsyncDatabaseWriter {};

/// This class is used by pipeline elements to send data to
/// a concurrent queue that is processed asynchronously by
/// the DB thread.
template <typename App, typename Input, typename Output>
class Task<AsyncDatabaseWriter<App, Input, Output>> : public NonTerminalTask<Input, Output>
{
private:
    using Func = std::function<RunnableOutcome(Input&&, ConcurrentQueue<Output>&, AppPreparedINSERTs*, bool)>;

    /// Not meant to be publicly constructible.
    Task(DatabaseManager* db_mgr, AppPreparedINSERTs&& app_tables, Func func)
        : func_(func)
        , db_mgr_(db_mgr)
        , app_tables_(std::move(app_tables))
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
            auto o = func_(std::move(in), this->output_queue_->get(), &app_tables_, force);
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
            auto o = func_(std::move(in), this->output_queue_->get(), &app_tables_, force);
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
        return "AsyncDatabaseWriter<" + demangle_type<Input>() + ", " + demangle_type<Output>() + ">";
    }

    Func func_;
    DatabaseManager* db_mgr_ = nullptr;
    AppPreparedINSERTs app_tables_;
};

/// Specialization for terminal database writers.
template <typename App, typename Input>
class Task<AsyncDatabaseWriter<App, Input, void>> : public TerminalTask<Input>
{
private:
    using Func = std::function<RunnableOutcome(Input&&, AppPreparedINSERTs*, bool)>;

    /// Not meant to be publicly constructible.
    Task(DatabaseManager* db_mgr, AppPreparedINSERTs&& app_tables, Func func)
        : func_(func)
        , db_mgr_(db_mgr)
        , app_tables_(std::move(app_tables))
    {}

    friend class AsyncDatabaseAccessor;

    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), &app_tables_, force);
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
            auto o = func_(std::move(in), &app_tables_, force);
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
        return "AsyncDatabaseWriter<" + demangle_type<Input>() + ", void>";
    }

    Func func_;
    DatabaseManager* db_mgr_ = nullptr;
    AppPreparedINSERTs app_tables_;
};

} // namespace simdb::pipeline
