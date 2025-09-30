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
    using Func = std::function<void(Input&&, ConcurrentQueue<Output>&, DatabaseManager*, bool)>;

    /// Not meant to be publicly constructible.
    Task(DatabaseManager* db_mgr, Func func)
        : func_(func)
        , db_mgr_(db_mgr)
    {}

    friend class AsyncDatabaseAccessor;

    /// Process one item from the queue. Always invoked on the database thread.
    bool run(bool force_flush) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        bool ran = false;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), this->output_queue_->get(), db_mgr_, force_flush);
            ran = true;
        }
        return ran;
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
    using Func = std::function<void(Input&&, DatabaseManager*, bool)>;

    /// Not meant to be publicly constructible.
    Task(DatabaseManager* db_mgr, Func func)
        : func_(func)
        , db_mgr_(db_mgr)
    {}

    friend class AsyncDatabaseAccessor;

    /// Process one item from the queue. Always invoked on the database thread.
    bool run(bool force_flush) override
    {
        bool ran = false;
        Input in;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), db_mgr_, force_flush);
            ran = true;
        }
        return ran;
    }

    std::string getDescription_() const override
    {
        return "AsyncDatabaseReader<" + demangle_type<Input>() + ", void>";
    }

    Func func_;
    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb::pipeline
