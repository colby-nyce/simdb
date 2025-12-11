// <AsyncDatabaseAccessor.hpp> -*- C++ -*-

#pragma once

#include <functional>
#include <future>
#include <memory>
#include <string>

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// Function that is queued by any pipeline thread (or the main
/// thread) and invoked on the dedicated database thread.
using AsyncDbAccessFunc = std::function<void(DatabaseManager*)>;

/// This struct is used to wrap a DB access function and
/// an exception that is suitable for std::future/promise.
struct AsyncDatabaseTask
{
    AsyncDbAccessFunc func;
    std::promise<std::string> exception_reason;

    AsyncDatabaseTask(AsyncDbAccessFunc func) : func(func) {}
    AsyncDatabaseTask() = default;
};

using AsyncDatabaseTaskPtr = std::shared_ptr<AsyncDatabaseTask>;

/// Base class to be implemented by the DatabaseThread.
class AsyncDatabaseAccessHandler
{
public:
    virtual ~AsyncDatabaseAccessHandler() = default;

    /// Put a task on the DB thread for evaluation, and BLOCK
    /// until it is called. The DB thread will complete its
    /// current transaction (INSERTs) immediately and evaluate
    /// this task inside a separate safeTransaction().
    ///
    /// If a nonzero timeout is given, throws a DBException
    /// if the task is not completed within the timeout.
    virtual void eval(AsyncDatabaseTaskPtr&& task, double timeout_seconds = 0) = 0;
};

/// This class is used by SimDB apps and pipeline elements to
/// asynchronously access the database. It supports async data
/// writes and enqueuing general-purpose std::functions.
class AsyncDatabaseAccessor
{
public:
    /// Invoke a std::function on the DB thread. This BLOCKS the
    /// calling thread until the function is processed.
    /// (Uses std::future/promise).
    ///
    /// If a nonzero timeout is given, throws a DBException
    /// if the task is not completed within the timeout.
    void eval(const AsyncDbAccessFunc& func, double timeout_seconds = 0)
    {
        auto task = std::make_shared<AsyncDatabaseTask>(func);
        db_access_handler_->eval(std::move(task), timeout_seconds);
    }

private:
    AsyncDatabaseAccessor(AsyncDatabaseAccessHandler* db_access_handler)
        : db_access_handler_(db_access_handler)
    {}

    AsyncDatabaseAccessHandler* db_access_handler_ = nullptr;
    friend class DatabaseThread;
};

} // namespace simdb::pipeline
