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
    virtual void eval(AsyncDatabaseTaskPtr&& task) = 0;
};

/// This class is used by SimDB apps and pipeline elements to
/// asynchronously access the database. It supports async data
/// writes and enqueuing general-purpose std::functions.
class AsyncDatabaseAccessor
{
public:
    /// Create an entry point for asynchronous database writes
    template <typename App, typename Input, typename Output, typename... Args>
    Task<AsyncDatabaseWriter<App, Input, Output>>* createAsyncWriter(Args&&... args)
    {
        auto db_mgr = db_access_handler_->getDatabaseManager();

        Schema schema;
        App::defineSchema(schema);

        // Create all PreparedINSERT objects for this App's schema.
        typename AppPreparedINSERTs::TableInserters inserters;
        for (const auto& tbl : schema.getTables())
        {
            std::vector<std::string> col_names;
            for (const auto& col : tbl.getColumns())
            {
                col_names.emplace_back(col->getName());
            }

            SqlTable table(tbl.getName());
            SqlColumns columns(col_names);
            auto inserter = db_mgr->prepareINSERT(std::move(table), std::move(columns));
            inserters[tbl.getName()] = std::move(inserter);
        }

        std::unique_ptr<Task<AsyncDatabaseWriter<App, Input, Output>>> writer(
            new Task<AsyncDatabaseWriter<App, Input, Output>>(
                db_mgr, std::move(inserters), std::forward<Args>(args)...));

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
