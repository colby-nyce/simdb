// <DatabaseTask.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"

namespace simdb::pipeline {

/// Async DB element.
template <typename Input, typename Output>
class DatabaseTask {};

/// Base class for database tasks. Provides access to the
/// DatabaseManager as well as prepared INSERT objects that
/// are specific to a particular App's schema.
class DatabaseAccessor
{
public:
    DatabaseAccessor(DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    template <typename App>
    PreparedINSERT& getTableInserter(const std::string& tbl_name)
    {
        auto& inserters = tbl_inserters_by_app_[App::NAME];
        if (inserters.empty())
        {
            Schema schema;
            App::defineSchema(schema);

            for (const auto& tbl : schema.getTables())
            {
                std::vector<std::string> col_names;
                for (const auto& col : tbl.getColumns())
                {
                    col_names.emplace_back(col->getName());
                }

                SqlTable table(tbl.getName());
                SqlColumns columns(col_names);
                auto inserter = db_mgr_->prepareINSERT(std::move(table), std::move(columns));
                inserters[tbl.getName()] = std::move(inserter);
            }
        }

        auto& inserter = inserters[tbl_name];
        if (!inserter)
        {
            throw DBException("Table does not exist in schema for app: ")
                << App::NAME;
        }

        return *inserter;
    }

private:
    DatabaseManager* db_mgr_ = nullptr;

    using TableInserters = std::unordered_map<std::string, std::unique_ptr<PreparedINSERT>>;
    std::unordered_map<std::string, TableInserters> tbl_inserters_by_app_;
};

/// This class is used to create async DB read/write tasks
/// for SimDB pipelines. These will always be executed on
/// the DB thread.
template <typename Input, typename Output>
class Task<DatabaseTask<Input, Output>> : public NonTerminalTask<Input, Output>
                                        , public DatabaseAccessor
{
public:
    using Func = std::function<RunnableOutcome(Input&&, ConcurrentQueue<Output>&, DatabaseAccessor&, bool)>;

    Task(DatabaseManager* db_mgr, Func func)
        : DatabaseAccessor(db_mgr)
        , func_(func)
    {}

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
            auto o = func_(std::move(in), this->output_queue_->get(), *this, force);
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
            auto o = func_(std::move(in), this->output_queue_->get(), *this, force);
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

private:
    std::string getDescription_() const override
    {
        return "DatabaseTask<" + demangle_type<Input>() + ", " + demangle_type<Output>() + ">";
    }

    Func func_;
};

/// Specialization: Input without an output
template <typename Input>
class Task<DatabaseTask<Input, void>> : public TerminalTask<Input>
                                      , public DatabaseAccessor
{
public:
    using Func = std::function<RunnableOutcome(Input&&, DatabaseAccessor&, bool)>;

    Task(DatabaseManager* db_mgr, Func func)
        : DatabaseAccessor(db_mgr)
        , func_(func)
    {}

    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            auto o = func_(std::move(in), *this, force);
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
            auto o = func_(std::move(in), *this, force);
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

private:
    std::string getDescription_() const override
    {
        return "DatabaseTask<" + demangle_type<Input>() + ", void>";
    }

    Func func_;
};

/// Specialization: Output without an input
template <typename Output>
class Task<DatabaseTask<void, Output>> : public TaskBase
                                       , public DatabaseAccessor
{
public:
    using Func = std::function<RunnableOutcome(ConcurrentQueue<Output>&, DatabaseAccessor&, bool)>;

    Task(DatabaseManager* db_mgr, Func func)
        : DatabaseAccessor(db_mgr)
        , func_(func)
    {}

    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        auto o = func_(this->output_queue_->get(), *this, force);
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
        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        size_t num_processed = 0;
        while (true)
        {
            auto outcome = this->processOne(force);
            if (outcome == RunnableOutcome::ABORT_FLUSH)
            {
                return outcome;
            }
            if (outcome == RunnableOutcome::DID_WORK)
            {
                ++num_processed;
            }
            if (outcome == RunnableOutcome::NO_OP)
            {
                break;
            }
        }

        return num_processed ? RunnableOutcome::DID_WORK : RunnableOutcome::NO_OP;
    }

private:
    QueueBase* getInputQueue() override final
    {
        return nullptr;
    }

    void setOutputQueue(QueueBase* queue) override final
    {
        if (auto q = dynamic_cast<Queue<Output>*>(queue))
        {
            this->output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    std::string getDescription_() const override
    {
        return "DatabaseTask<void, " + demangle_type<Output>() + ">";
    }

    Func func_;
    Queue<Output>* output_queue_ = nullptr;
};

/// Specialization: No input, no output
template <>
class Task<DatabaseTask<void, void>> : public TaskBase
                                     , public DatabaseAccessor
{
public:
    using Func = std::function<RunnableOutcome(DatabaseAccessor&, bool)>;

    Task(DatabaseManager* db_mgr, Func func)
        : DatabaseAccessor(db_mgr)
        , func_(func)
    {}

    QueueBase* getInputQueue() override final
    {
        return nullptr;
    }

    void setOutputQueue(QueueBase*) override final
    {
        throw DBException("Cannot set output queue (task has no output)");
    }

    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        auto o = func_(*this, force);
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
        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        size_t num_processed = 0;
        while (true)
        {
            auto outcome = processOne(force);
            if (outcome == RunnableOutcome::ABORT_FLUSH)
            {
                return outcome;
            }
            if (outcome == RunnableOutcome::DID_WORK)
            {
                ++num_processed;
            }
            if (outcome == RunnableOutcome::NO_OP)
            {
                break;
            }
        }

        return num_processed ? RunnableOutcome::DID_WORK : RunnableOutcome::NO_OP;
    }

private:
    std::string getDescription_() const override
    {
        return "DatabaseTask<void, void>";
    }

    Func func_;
};

} // namespace simdb::pipeline
