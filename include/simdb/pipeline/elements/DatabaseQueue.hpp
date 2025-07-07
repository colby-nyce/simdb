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
class Task<DatabaseQueue<DatabaseIn, DatabaseOut>> : public NonTerminalDatabaseTask<DatabaseIn>
{
public:
    using DbFunc = std::function<void(DatabaseIn&&, ConcurrentQueue<DatabaseOut>&, DatabaseManager*)>;
    Task(DbFunc func) : func_(func) {}

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

    bool run() override
    {
        DatabaseIn in;
        bool ran = false;
        while (this->input_queue_.get().try_pop(in))
        {
            func_(std::move(in), output_queue_->get(), this->getDatabaseManager_());
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
    Queue<DatabaseOut>* output_queue_ = nullptr;
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
        while (this->input_queue_.get().try_pop(in))
        {
            func_(std::move(in), this->getDatabaseManager_());
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
};

} // namespace simdb::pipeline
