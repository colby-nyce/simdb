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

template <typename InputType>
class DatabaseQueue
{
public:

private:
};

/// The database thread is used to ensure that Runnable::run()
/// methods are grouped inside BEGIN/COMMIT TRANSACTION blocks
/// for much better performance.
///
/// It also provides asynchronous access to the database for
/// async pipeline queries or serialized data writes.
class DatabaseThread : private AsyncDatabaseAccessHandler, public PollingThread
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
    void eval(AsyncDatabaseTaskPtr&& task) override final
    {
        dormant_thread_.eval(std::move(task));
    }

    /// Overridden from PollingThread
    bool run_() override final
    {
        bool ran = false;

        db_mgr_->safeTransaction(
            [&]()
            {
                bool continue_while;
                do
                {
                    continue_while = false;
                    for (auto& runnable : polling_runnables_)
                    {
                        continue_while |= runnable->run();

                        if (dormant_thread_.hasTasks())
                        {
                            continue_while = false;
                            break;
                        }
                    }
                    ran |= continue_while;
                } while (continue_while);
            });

        return ran;
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

        void eval(AsyncDatabaseTaskPtr&& task)
        {
            std::future<std::string> fut = task->exception_reason.get_future();
            pending_async_db_tasks_.emplace(std::move(task));
            cond_var_.notify_one();

            // Block until DB thread sets result
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
