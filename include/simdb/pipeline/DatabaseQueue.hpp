// <DatabaseQueue.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Runnable.hpp"
#include "simdb/pipeline/Thread.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"

namespace simdb::pipeline {

/// The DatabaseQueue gives you a way to send data to a
/// thread-safe queue. The data is processed on the shared
/// DatabaseThread no matter how many DatabaseQueue's there
/// are.
///
///     DatabaseManager db_mgr("sim.db");
///     ... create schema ...
///
///     DatabaseThread db_thread(&db_mgr);
///
///     DatabaseQueue<std::vector<double>> doublesQ(db_thread,
///         [](std::vector<double>&& values, DatabaseManager* db_mgr)
///         {
///              db_mgr->INSERT(
///                  SQL_TABLE("DoubleValues"),
///                  SQL_COLUMNS("ValBlob"),
///                  SQL_VALUES(values));
///         });
///
///     DatabaseQueue<std::string> stringsQ(db_thread,
///         [](std::string&& s, DatabaseManager* db_mgr)
///         {
///              db_mgr->INSERT(
///                  SQL_TABLE("StringValues"),
///                  SQL_COLUMNS("StringVal"),
///                  SQL_VALUES(s));
///         });
///
///     db_thread.open();
///     doublesQ.process({1.1,2.2,3.3});
///     stringsQ.process("HelloWorld");
///     db_thread.close();
///
template <typename DatabaseIn>
class DatabaseQueue : public Runnable
{
public:
    /// Function to be called as often as there is data available.
    /// Invoked on the database thread.
    using DatabaseFunc = std::function<void(DatabaseIn&&, DatabaseManager*)>;

    DatabaseQueue(DatabaseThread& db_thread, DatabaseFunc db_func)
        : db_func_(db_func)
        , db_mgr_(db_thread.getDatabaseManager())
    {
        db_thread.addRunnable(this);
    }

    /// Move data into the database queue.
    void process(DatabaseIn&& in)
    {
        input_queue_.emplace(std::move(in));
    }

    /// Copy data into the database queue.
    void process(const DatabaseIn& in)
    {
        input_queue_.push(in);
    }

private:
    /// Called on the shared database thread. When we run out of
    /// data to process, the thread will sleep briefly before
    /// calling run() again.
    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (input_queue_.try_pop(in))
        {
            db_func_(std::move(in), db_mgr_);
            ran = true;
        }
        return ran;
    }

    std::string getName_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ">";
    }

    DatabaseFunc db_func_;
    DatabaseManager* db_mgr_ = nullptr;
    ConcurrentQueue<DatabaseIn> input_queue_;
};

} // namespace simdb::pipeline
