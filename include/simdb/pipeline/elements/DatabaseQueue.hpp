// <DatabaseQueue.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/PreparedINSERT.hpp"
#include "simdb/pipeline/Runnable.hpp"
#include "simdb/pipeline/Thread.hpp"
#include "simdb/pipeline/Task.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"

namespace simdb::pipeline {

template <typename DatabaseIn, typename DatabaseOut>
class DatabaseQueue {};

template <typename DatabaseIn, typename DatabaseOut>
class Task<DatabaseQueue<DatabaseIn, DatabaseOut>> : public NonTerminalDatabaseTask<DatabaseIn, DatabaseOut>
{
public:
    using DbFunc = std::function<void(DatabaseIn&&, ConcurrentQueue<DatabaseOut>&, PreparedINSERT*)>;
    Task(SqlTable&& table, SqlColumns&& cols, DbFunc func)
        : table_(std::move(table))
        , columns_(std::move(cols))
        , func_(func)
    {}

    using TaskBase::getTypedInputQueue;

    bool run() override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        if (!inserter_)
        {
            inserter_ = this->getDatabaseManager_()->prepareINSERT(std::move(table_), std::move(columns_));
        }

        DatabaseIn in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), this->output_queue_->get(), inserter_.get());
            ran = true;
        }
        return ran;
    }

private:
    std::string getDescription_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", " + demangle_type<DatabaseOut>() + ">";
    }

    SqlTable table_;
    SqlColumns columns_;
    DbFunc func_;
    std::unique_ptr<PreparedINSERT> inserter_;
};

template <typename DatabaseIn>
class Task<DatabaseQueue<DatabaseIn, void>> : public TerminalDatabaseTask<DatabaseIn>
{
public:
    using DbFunc = std::function<void(DatabaseIn&&, PreparedINSERT*)>;
    Task(SqlTable&& table, SqlColumns&& cols, DbFunc func)
        : table_(std::move(table))
        , columns_(std::move(cols))
        , func_(func)
    {}

    bool run() override
    {
        if (!inserter_)
        {
            inserter_ = this->getDatabaseManager_()->prepareINSERT(std::move(table_), std::move(columns_));
        }

        DatabaseIn in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), inserter_.get());
            ran = true;
        }
        return ran;
    }

private:
    std::string getDescription_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", void>";
    }

    SqlTable table_;
    SqlColumns columns_;
    DbFunc func_;
    std::unique_ptr<PreparedINSERT> inserter_;
};

} // namespace simdb::pipeline
