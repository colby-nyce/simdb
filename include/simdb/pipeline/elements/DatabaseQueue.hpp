// <DatabaseQueue.hpp> -*- C++ -*-

#pragma once

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
    using DbFunc = std::function<void(DatabaseIn&&, ConcurrentQueue<DatabaseOut>&, DatabaseManager*)>;
    Task(DbFunc func) : func_(func) {}

    using TaskBase::getTypedInputQueue;

    bool run() override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        DatabaseIn in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), this->output_queue_->get(), this->getDatabaseManager_());
            ran = true;
        }
        return ran;
    }

private:
    std::string getDescription_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", " + demangle_type<DatabaseOut>() + ">";
    }

    DbFunc func_;
};

template <typename DatabaseIn>
class Task<DatabaseQueue<DatabaseIn, void>> : public TerminalDatabaseTask<DatabaseIn>
{
public:
    using DbFunc = std::function<void(DatabaseIn&&, DatabaseManager*)>;
    Task(DbFunc func) : func_(func) {}

    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            func_(std::move(in), this->getDatabaseManager_());
            ran = true;
        }
        return ran;
    }

private:
    std::string getDescription_() const override
    {
        return "DatabaseQueue<" + demangle_type<DatabaseIn>() + ", void>";
    }

    DbFunc func_;
};

} // namespace simdb::pipeline
