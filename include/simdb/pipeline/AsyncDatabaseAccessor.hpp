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

/// \brief Callable queued by any thread and invoked on the dedicated database thread (receives DatabaseManager*).
using AsyncDbAccessFunc = std::function<void(DatabaseManager*)>;

/*!
 * \struct AsyncDatabaseTask
 *
 * \brief Wraps an AsyncDbAccessFunc and a std::promise for the exception message;
 *        used to run the function on the DB thread and propagate exceptions to the caller.
 */
struct AsyncDatabaseTask
{
    AsyncDbAccessFunc func;
    std::promise<std::string> exception_reason;

    explicit AsyncDatabaseTask(AsyncDbAccessFunc func) :
        func(func)
    {
    }
    AsyncDatabaseTask() = default;
};

using AsyncDatabaseTaskPtr = std::shared_ptr<AsyncDatabaseTask>;

/*!
 * \class AsyncDatabaseAccessHandler
 *
 * \brief Interface implemented by DatabaseThread. Receives tasks and runs them
 *        on the DB thread inside safeTransaction(); callers block until the
 *        task completes (or timeout).
 */
class AsyncDatabaseAccessHandler
{
public:
    virtual ~AsyncDatabaseAccessHandler() = default;

    /// \brief Run the task on the DB thread; block until done (or timeout).
    /// \param task Task to run inside a safeTransaction().
    /// \param timeout_seconds If > 0, throw if not completed within this many seconds.
    /// \throws DBException on timeout or if the task throws.
    virtual void eval(AsyncDatabaseTaskPtr&& task, double timeout_seconds = 0) = 0;
};

/*!
 * \class AsyncDatabaseAccessor
 *
 * \brief Handle for pipeline stages and apps to run code on the dedicated
 *        database thread. eval(func) blocks the caller until func(DatabaseManager*)
 *        has run. Obtained from PipelineManager::getAsyncDatabaseAccessor() or
 *        DatabaseThread::getAsyncDatabaseAccessor().
 */
class AsyncDatabaseAccessor
{
public:
    /// \brief Run \p func on the DB thread; block until it completes (or timeout).
    /// \param func Callable that receives the DatabaseManager*.
    /// \param timeout_seconds If > 0, throw if not completed within this many seconds.
    /// \throws DBException on timeout or if \p func throws.
    void eval(const AsyncDbAccessFunc& func, double timeout_seconds = 0)
    {
        auto task = std::make_shared<AsyncDatabaseTask>(func);
        db_access_handler_->eval(std::move(task), timeout_seconds);
    }

private:
    AsyncDatabaseAccessor(AsyncDatabaseAccessHandler* db_access_handler) :
        db_access_handler_(db_access_handler)
    {
    }

    AsyncDatabaseAccessHandler* db_access_handler_ = nullptr;
    friend class DatabaseThread;
};

} // namespace simdb::pipeline
