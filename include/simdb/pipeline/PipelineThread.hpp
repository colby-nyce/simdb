#pragma once

#include "simdb/pipeline/PipelineRunnable.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

#include <atomic>
#include <chrono>
#include <thread>

namespace simdb::pipeline {

class Thread
{
public:
    Thread(const size_t interval_milliseconds = 100)
        : interval_ms_(interval_milliseconds)
    {
    }

    ~Thread() noexcept
    {
        close();
    }

    void addRunnable(pipeline::Runnable* runnable)
    {
        if (is_running_)
        {
            throw DBException("Cannot add runnables while thread is running");
        }
        runnables_.emplace_back(runnable);
    }

    void open()
    {
        if (runnables_.empty())
        {
            throw DBException("No runnables have been assigned to this thread");
        }

        if (!is_running_)
        {
            is_running_ = true;
            thread_ = std::make_unique<std::thread>(&Thread::run_, this);
        }
    }

    void close() noexcept
    {
        if (is_running_)
        {
            is_running_ = false;
            if (thread_->joinable())
            {
                thread_->join();
                thread_.reset();
            }
        }
    }

protected:
    virtual bool flush_()
    {
        bool flushed = false;
        for (auto runner : runnables_)
        {
            flushed |= runner->run();
        }
        return flushed;
    }

private:
    void run_()
    {
        while (is_running_)
        {
            if (!flush_())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
            }
        }

        flush_();
    }

    const size_t interval_ms_;
    std::vector<pipeline::Runnable*> runnables_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> is_running_ = false;
};

class DatabaseThread : public Thread
{
public:
    DatabaseThread(DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

private:
    bool flush_() override
    {
        bool ran = false;
        db_mgr_->safeTransaction([&]() { ran = Thread::flush_(); });
        return ran;
    }

    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb::pipeline
