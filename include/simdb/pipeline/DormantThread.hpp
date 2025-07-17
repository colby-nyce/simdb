// <DormantThread.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/AsyncDatabaseTask.hpp"
#include <queue>

namespace simdb::pipeline {

/// This thread class is used for lower-volume data than the PollingThread,
/// and uses condition variables to wake up on demand.
class DormantThread
{
public:
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

    void eval(const std::string& cmd)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cmd_queue_.push(cmd);
        }
        cond_var_.notify_one();
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

    void printPerfReport(std::ostream& os) const noexcept
    {
        (void)os;
    }

private:
    void loop_()
    {
        while (true)
        {
            
        }
    }

    std::unique_ptr<std::thread> thread_;
    std::mutex mutex_;
    std::condition_variable cond_var_;
    std::atomic<bool> stop_;
    std::queue<std::string> cmd_queue_;
};

} // namespace simdb::pipeline
