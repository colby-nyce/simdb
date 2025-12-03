// <AsyncDatabaseAccessor.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/Task.hpp"
#include "simdb/pipeline/AsyncDatabaseTask.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"

#include <functional>
#include <future>

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// Base class to be implemented by the DatabaseThread.
class AsyncDatabaseAccessHandler
{
public:
    virtual ~AsyncDatabaseAccessHandler() = default;

    /// Get the SimDB instance tied to the DatabaseThread.
    virtual DatabaseManager* getDatabaseManager() const = 0;

    /// Add a runnable to the database thread. All run() methods
    /// are implicitly called inside safeTransaction().
    virtual void addRunnable(std::unique_ptr<Runnable> runnable) = 0;

    /// Put a task on the DB thread for evaluation, and BLOCK
    /// until it is called. The DB thread will complete its
    /// current transaction (INSERTs) immediately and evaluate
    /// this task inside a separate safeTransaction().
    ///
    /// If a nonzero timeout is given, throws a DBException
    /// if the task is not completed within the timeout.
    virtual void eval(AsyncDatabaseTaskPtr&& task, double timeout_seconds = 0) = 0;
};

/// Forward declaration
template <typename Input, typename Output>
class DatabaseTask;

/// This class is used by SimDB apps and pipeline elements to
/// asynchronously access the database. It supports async data
/// writes and enqueuing general-purpose std::functions.
class AsyncDatabaseAccessor
{
public:
    /// Assign a DatabaseTask to the DB thread.
    template <typename Input, typename Output>
    void addTask(std::unique_ptr<Task<DatabaseTask<Input, Output>>> task)
    {
        db_access_handler_->addRunnable(std::move(task));
    }

    /// Assign multiple DatabaseTasks to the DB thread.
    template <typename Input, typename Output, typename... Rest>
    void addTasks(std::unique_ptr<Task<DatabaseTask<Input, Output>>> task, Rest&&... rest)
    {
        addTask(std::move(task));
        addTasks(std::forward<Rest>(rest)...);
    }

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

    /// Base case for template unrolling
    void addTasks() {}

    AsyncDatabaseAccessHandler* db_access_handler_ = nullptr;
    friend class DatabaseThread;
};

} // namespace simdb::pipeline
