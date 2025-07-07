// <DatabaseQueue.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Runnable.hpp"
#include "simdb/pipeline/Thread.hpp"
#include "simdb/pipeline/Task.hpp"
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
template <typename DatabaseIn, typename DatabaseOut>
class DatabaseQueue {};

template <typename DatabaseIn>
class Task<DatabaseQueue<DatabaseIn, void>> : public TaskBase
{
public:
    using DbFunc = std::function<void(DatabaseIn&&, DatabaseManager*)>;
    Task(DbFunc func) : func_(func) {}

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return nullptr;
    }

    void setOutputQueue(QueueBase*) override
    {
        throw DBException("Cannot set output queue - this is a terminating DB task");
    }

    bool requiresDatabase() const override
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override
    {
        db_mgr_ = db_mgr;
    }

    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            func_(std::move(in), db_mgr_);
            ran = true;
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", void>";
    }

    DbFunc func_;
    DatabaseManager* db_mgr_ = nullptr;
    Queue<DatabaseIn> input_queue_;
};

template <typename DatabaseIn, typename DatabaseOut>
class Task<DatabaseQueue<DatabaseIn, DatabaseOut>> : public TaskBase
{
public:
    using DbFunc = std::function<void(DatabaseIn&&, ConcurrentQueue<DatabaseOut>&, DatabaseManager*)>;
    Task(DbFunc func) : func_(func) {}

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
    }

    QueueBase* getOutputQueue() override
    {
        return output_queue_;
    }

    void setOutputQueue(QueueBase* queue) override
    {
        if (auto q = dynamic_cast<Queue<DatabaseOut>*>(queue))
        {
            output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    bool requiresDatabase() const override
    {
        return true;
    }

    void setDatabaseManager(DatabaseManager* db_mgr) override
    {
        db_mgr_ = db_mgr;
    }

    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            func_(std::move(in), output_queue_->get(), db_mgr_);
            ran = true;
        }
        return ran;
    }

private:
    std::string getName_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", " + demangle_type<DatabaseOut>() + ">";
    }

    DbFunc func_;
    DatabaseManager* db_mgr_ = nullptr;
    Queue<DatabaseIn> input_queue_;
    Queue<DatabaseOut>* output_queue_ = nullptr;
};

} // namespace simdb::pipeline
