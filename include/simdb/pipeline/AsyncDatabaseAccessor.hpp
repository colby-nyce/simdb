// <AsyncDatabaseAccessor.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/elements/AsyncDbWriter.hpp"
#include "simdb/pipeline/AsyncDatabaseTask.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"

#include <functional>
#include <future>

namespace simdb {
    class DatabaseManager;
}

namespace simdb::pipeline {

/// Base class to be implemented by the DatabaseThread. Used to
/// queue DB accesses and service them on the DB thread.
class AsyncDatabaseAccessHandler
{
public:
    virtual ~AsyncDatabaseAccessHandler() = default;
    virtual DatabaseManager* getDatabaseManager() const = 0;
    virtual void addRunnable(std::unique_ptr<Runnable> runnable) = 0;
    virtual void eval(AsyncDatabaseTaskPtr&& task) = 0;
};

/// This class is used by SimDB apps and pipeline elements to
/// asynchronously access the database. It supports async data
/// writes and enqueuing general-purpose std::functions.
class AsyncDatabaseAccessor
{
public:
    /// Create an entry point for asynchronous database writes
    template <typename Input, typename Output, typename... Args>
    Task<AsyncDatabaseWriter<Input, Output>>* createAsyncWriter(
        SqlTable&& table, SqlColumns&& cols, Args&&... args)
    {
        auto db_mgr = db_access_handler_->getDatabaseManager();
        auto inserter = db_mgr->prepareINSERT(std::move(table), std::move(cols));

        std::unique_ptr<Task<AsyncDatabaseWriter<Input, Output>>> writer(
            new Task<AsyncDatabaseWriter<Input, Output>>(
                std::move(inserter), std::forward<Args>(args)...));

        auto ret = writer.get();
        db_access_handler_->addRunnable(std::move(writer));
        return ret;
    }

    /// Invoke a std::function on the DB thread. This BLOCKS the
    /// calling thread until the function is processed.
    /// (Uses std::future/promise).
    void eval(const AsyncDbAccessFunc& func)
    {
        auto task = std::make_shared<AsyncDatabaseTask>(func);
        db_access_handler_->eval(std::move(task));
    }

private:
    AsyncDatabaseAccessor(AsyncDatabaseAccessHandler* db_access_handler)
        : db_access_handler_(db_access_handler)
    {}

    AsyncDatabaseAccessHandler* db_access_handler_ = nullptr;
    friend class DatabaseThread;
};

} // namespace simdb::pipeline
