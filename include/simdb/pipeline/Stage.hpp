// <Stage.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/QueueRepo.hpp"
#include "simdb/pipeline/Runnable.hpp"
#include "simdb/pipeline/PollingThread.hpp"
#include "simdb/pipeline/DatabaseThread.hpp"
#include "simdb/pipeline/DatabaseAccessor.hpp"
#include "simdb/Exceptions.hpp"
#include <memory>
#include <unordered_map>

namespace simdb::pipeline {

class Stage : public Runnable
{
protected:
    Stage(const std::string& name, QueueRepo& queue_repo)
        : name_(name)
        , queue_repo_(queue_repo)
    {}

    template <typename T>
    void addInPort_(const std::string& port_name, ConcurrentQueue<T>*& queue)
    {
        queue_repo_.addInPortPlaceholder<T>(name_, port_name, queue);
    }

    template <typename T>
    void addOutPort_(const std::string& port_name, ConcurrentQueue<T>*& queue)
    {
        queue_repo_.addOutPortPlaceholder<T>(name_, port_name, queue);
    }

    virtual AsyncDatabaseAccessor* getAsyncDatabaseAccessor_() const
    {
        return async_db_accessor_;
    }

private:
    virtual void assignThread(DatabaseManager*, std::vector<std::unique_ptr<PollingThread>>& threads, AsyncDatabaseAccessor*&)
    {
        // Create a new thread if none exist or if we don't share threads
        if (threads.empty() || !shareThreads_())
        {
            threads.emplace_back(std::make_unique<PollingThread>());
            threads.back()->addRunnable(this);
            return;
        }

        // Add this stage to the thread that has the fewest runnables
        PollingThread* avail_thread = nullptr;
        size_t min_runnables = SIZE_MAX;
        for (auto& thread : threads)
        {
            if (dynamic_cast<DatabaseThread*>(thread.get()))
            {
                // Cannot add to the DB thread
                continue;
            }

            auto num_runnables = thread->getNumRunnables();
            if (num_runnables < min_runnables)
            {
                min_runnables = num_runnables;
                avail_thread = thread.get();
            }
        }

        if (!avail_thread)
        {
            // The only instantiated thread is the DatabaseThread so far.
            // Create a non-DB thread.
            threads.emplace_back(std::make_unique<PollingThread>());
            avail_thread = threads.back().get();
        }

        avail_thread->addRunnable(this);
    }

    void setAsyncDatabaseAccessor_(AsyncDatabaseAccessor* async_db_accessor)
    {
        async_db_accessor_ = async_db_accessor;
    }

    std::string getDescription_() const override {
        return name_;
    }

    RunnableOutcome processOne(bool force) override final
    {
        return run_(force);
    }

    RunnableOutcome processAll(bool force) override final
    {
        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        while (true)
        {
            auto result = processOne(force);
            if (result == RunnableOutcome::DID_WORK)
            {
                outcome = RunnableOutcome::DID_WORK;
            }
            else if (result == RunnableOutcome::NO_OP)
            {
                break;
            }
        }
        return outcome;
    }

    virtual RunnableOutcome run_(bool force) = 0;

    virtual bool shareThreads_() const
    {
        return true;
    }

    std::string name_;
    QueueRepo& queue_repo_;
    AsyncDatabaseAccessor* async_db_accessor_ = nullptr;

    friend class Pipeline;
    friend class Flusher;
};

class DatabaseStageBase : public Stage
{
public:
    DatabaseStageBase(const std::string& name, QueueRepo& queue_repo)
        : Stage(name, queue_repo)
    {}

protected:
    AsyncDatabaseAccessor* getAsyncDatabaseAccessor_() const override final
    {
        throw DBException("Cannot access the AsyncDatabaseAccessor from a DatabaseStage - use getDatabaseManager_()");
    }
};

template <typename AppT>
class DatabaseStage : public DatabaseStageBase
{
public:
    DatabaseStage(const std::string& name, QueueRepo& queue_repo)
        : DatabaseStageBase(name, queue_repo)
    {}

protected:
    DatabaseManager* getDatabaseManager_()
    {
        if (!db_accessor_)
        {
            throw DBException("DatabaseAccessor not initialized");
        }
        return db_accessor_->getDatabaseManager();
    }

    PreparedINSERT* getTableInserter_(const std::string& tbl_name)
    {
        if (!db_accessor_)
        {
            throw DBException("DatabaseAccessor not initialized");
        }
        return db_accessor_->template getTableInserter<AppT>(tbl_name);
    }

private:
    void assignThread(DatabaseManager* db_mgr, std::vector<std::unique_ptr<PollingThread>>& threads, AsyncDatabaseAccessor*& async_db_accessor) override final
    {
        // Prepare the DatabaseAccessor
        db_accessor_ = std::make_unique<DatabaseAccessor>(db_mgr);

        // Look for dedicated database thread
        for (auto& thread : threads)
        {
            if (auto db_thread = dynamic_cast<DatabaseThread*>(thread.get()))
            {
                db_thread->addRunnable(this);
                async_db_accessor = db_thread->getAsyncDatabaseAccessor();
                return;
            }
        }

        // No dedicated database thread found, create a new one
        threads.emplace_back(std::make_unique<DatabaseThread>(db_mgr));
        threads.back()->addRunnable(this);
        async_db_accessor = dynamic_cast<DatabaseThread*>(threads.back().get())->getAsyncDatabaseAccessor();
    }

    bool shareThreads_() const override final
    {
        // Database stages never share threads
        return false;
    }

    std::unique_ptr<DatabaseAccessor> db_accessor_;
};

} // namespace simdb::pipeline
