// <DatabaseThread.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/pipeline/PollingThread.hpp"

namespace simdb {
    class AsyncDatabaseAccessor;
    class DatabaseManager;
}

namespace simdb::pipeline {

/// The database thread is used to ensure that Runnable::run()
/// methods are grouped inside BEGIN/COMMIT TRANSACTION blocks
/// for much better performance.
///
/// It also provides asynchronous access to the database for
/// async pipeline queries or serialized data writes.
class DatabaseThread : public PollingThread, private AsyncDatabaseAccessHandler
{
public:
    DatabaseThread(DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
        , dormant_thread_(db_mgr)
    {}

    ~DatabaseThread() noexcept = default;

    AsyncDatabaseAccessor* getAsyncDatabaseAccessor()
    {
        return &db_accessor_;
    }

private:
    /// Overridden from AsyncDatabaseAccessHandler
    DatabaseManager* getDatabaseManager() const override final
    {
        return db_mgr_;
    }

    /// Overridden from AsyncDatabaseAccessHandler
    void addRunnable(std::unique_ptr<Runnable> runnable) override final
    {
        PollingThread::addRunnable(runnable.get());
        polling_runnables_.emplace_back(std::move(runnable));
    }

    /// Overridden from AsyncDatabaseAccessHandler
    void eval(AsyncDatabaseTaskPtr&& task, double timeout_seconds = 0) override final
    {
        dormant_thread_.eval(std::move(task), timeout_seconds);
    }

    /// Overridden from PollingThread
    bool run_(bool force) override final
    {
        bool did_work = false;

        // Put all runnables (AsyncDbWriters) in a BEGIN/COMMIT block
        // for fastest insert performance.
        db_mgr_->safeTransaction(
            [&]()
            {
                bool continue_while;
                do
                {
                    continue_while = false;
                    for (auto& runnable : polling_runnables_)
                    {
                        if (!runnable->enabled())
                        {
                            continue;
                        }

                        // We call processOne() here instead of processAll() to use a smaller
                        // granularity of tasks to "inject" break statements more frequently
                        // in the event of pending async DB access requests.
                        if (runnable->processOne(force) == RunnableOutcome::DID_WORK)
                        {
                            continue_while = true;
                        }

                        // Give as high priority as possible for async DB access
                        if (dormant_thread_.hasTasks())
                        {
                            continue_while = false;
                            break;
                        }
                    }
                    did_work |= continue_while;
                } while (continue_while);
            });

        return did_work;
    }

    /// Overridden from PollingThread
    void open() override
    {
        PollingThread::open();
        dormant_thread_.open();
    }

    /// Overridden from PollingThread
    void close() noexcept override
    {
        PollingThread::close();
        dormant_thread_.close();
    }

    /// Overridden from PollingThread
    bool flushRunnables() override
    {
        bool did_work = false;

        db_mgr_->safeTransaction(
            [&]
            {
                did_work = PollingThread::flushRunnables();
            });

        return did_work;
    }

    /// Unlike the "always-on" PollingThread, the DormantThread
    /// class uses a condition variable to wake up and service
    /// async DB accesses before going back to sleep.
    class DormantThread
    {
    public:
        DormantThread(DatabaseManager* db_mgr)
            : db_mgr_(db_mgr)
        {}

        ~DormantThread() noexcept
        {
            close();
        }

        void open()
        {
            if (!thread_)
            {
                stop_ = false;
                thread_ = std::make_unique<std::thread>(&DormantThread::loop_, this);
            }
        }

        void close() noexcept
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stop_ = true;
            }
            cond_var_.notify_one();

            if (thread_)
            {
                if (thread_->joinable())
                {
                    thread_->join();
                }
                thread_.reset();
            }
        }

        void eval(AsyncDatabaseTaskPtr&& task, double timeout_seconds = 0)
        {
            if (stop_)
            {
                // If we are already stopped, we cannot use std::future. We have to
                // evaluate the task right here.
                db_mgr_->safeTransaction([&]() { task->func(db_mgr_); });
                return;
            }

            std::future<std::string> fut = task->exception_reason.get_future();
            pending_async_db_tasks_.emplace(std::move(task));
            cond_var_.notify_one();

            if (timeout_seconds > 0)
            {
                auto status = fut.wait_for(std::chrono::duration<double>(timeout_seconds));
                if (status == std::future_status::timeout)
                {
                    throw DBException("Timed out waiting for async DB task to complete");
                }
            }

            auto exception_reason = fut.get();
            if (!exception_reason.empty())
            {
                throw DBException(exception_reason);
            }
        }

        bool hasTasks() const
        {
            return !pending_async_db_tasks_.empty();
        }

    private:
        void loop_()
        {
            while (true)
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cond_var_.wait(lock, [this] { return !pending_async_db_tasks_.empty() || stop_; });

                // Assume we just woke up due to an async DB access request.
                // Immediately start a transaction so the DB polling thread
                // blocks until we are done responding to the requests.
                db_mgr_->safeTransaction(
                    [&]()
                    {
                        AsyncDatabaseTaskPtr async_task;
                        while (pending_async_db_tasks_.try_pop(async_task))
                        {
                            try
                            {
                                async_task->func(db_mgr_);
                                async_task->exception_reason.set_value("");
                            }
                            catch (const std::exception& ex)
                            {
                                async_task->exception_reason.set_value(ex.what());
                            }
                        }
                    });

                if (stop_)
                {
                    break;
                }
            }
        }

        DatabaseManager* db_mgr_ = nullptr;
        std::unique_ptr<std::thread> thread_;
        std::mutex mutex_;
        std::condition_variable cond_var_;
        std::atomic<bool> stop_;
        ConcurrentQueue<AsyncDatabaseTaskPtr> pending_async_db_tasks_;
    };

    DatabaseManager* db_mgr_ = nullptr;
    AsyncDatabaseAccessor db_accessor_{this};
    std::vector<std::unique_ptr<Runnable>> polling_runnables_;
    DormantThread dormant_thread_;
};

} // namespace simdb::pipeline
