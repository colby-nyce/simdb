// <Stage.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/QueueRepo.hpp"
#include "simdb/pipeline/Runnable.hpp"
#include "simdb/pipeline/PollingThread.hpp"
#include "simdb/pipeline/DatabaseThread.hpp"
#include "simdb/pipeline/elements/DatabaseTask.hpp"
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

private:
    virtual void assignThread(DatabaseManager*, std::vector<std::unique_ptr<PollingThread>>& threads)
    {
        // Create a new thread if none exist or if we don't share threads
        if (!shareThreads_() || threads.empty())
        {
            threads.emplace_back(std::make_unique<PollingThread>());
            threads.back()->addRunnable(this);
            return;
        }

        // Add this stage to the thread that has the fewest runnables
        auto min_thread_it = threads.begin();;
        size_t min_runnables = SIZE_MAX;
        for (auto it = threads.begin(); it != threads.end(); ++it)
        {
            size_t num_runnables = (*it)->getNumRunnables();
            if (num_runnables < min_runnables)
            {
                min_runnables = num_runnables;
                min_thread_it = it;;
            }
        }
        (*min_thread_it)->addRunnable(this);
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
            else if (result == RunnableOutcome::ABORT_FLUSH)
            {
                if (!force)
                {
                    throw DBException("ABORT_FLUSH returned during non-flush processing");
                }
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
    friend class Pipeline;
};

template <typename AppT>
class DatabaseStage : public Stage
{
public:
    DatabaseStage(const std::string& name, QueueRepo& queue_repo)
        : Stage(name, queue_repo)
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
    void assignThread(DatabaseManager* db_mgr, std::vector<std::unique_ptr<PollingThread>>& threads) override final
    {
        // Prepare the DatabaseAccessor
        db_accessor_ = std::make_unique<DatabaseAccessor>(db_mgr);

        // Look for dedicated database thread
        for (auto& thread : threads)
        {
            if (auto db_thread = dynamic_cast<DatabaseThread*>(thread.get()))
            {
                db_thread->addRunnable(this);
                return;
            }
        }

        // No dedicated database thread found, create a new one
        threads.emplace_back(std::make_unique<DatabaseThread>(db_mgr));
        threads.back()->addRunnable(this);
    }

    bool shareThreads_() const override final
    {
        // Database stages never share threads
        return false;
    }

    std::unique_ptr<DatabaseAccessor> db_accessor_;
};

} // namespace simdb::pipeline
