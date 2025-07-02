#pragma once

#include "simdb/pipeline/PipelineRunnable.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <iostream>

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
        printPerfReport(std::cout);
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
            return;
        }

        if (!is_running_)
        {
            is_running_ = true;
            start_ = std::chrono::high_resolution_clock::now();
            thread_ = std::make_unique<std::thread>(&Thread::loop_, this);
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

    void printPerfReport(std::ostream& os) const noexcept
    {
        auto now = std::chrono::high_resolution_clock::now();
        const std::chrono::duration<double> dur = now - start_;
        const double total_sleep_seconds = 1.0 * num_times_slept_ * interval_ms_ / 1000.0;
        const auto total_elap_seconds = dur.count();
        const auto pct_time_sleeping = (total_sleep_seconds / total_elap_seconds) * 100;
        const auto pct_time_working = 100 - pct_time_sleeping;

        // Thread containing:
        //   - <runnable>
        //   - <runnable>
        //
        // Performance report:
        //   Num times run:     548
        //   Pct time sleeping: 7.8%
        //   Pct time working:  91.2%

        os << "Thread containing:\n";
        for (const auto runnable : runnables_)
        {
            os << "    - " << runnable->getName() << "\n";
        }

        os << "\n";
        os << "    Performance report:\n";
        os << "        Num times run:      " << num_times_run_ << "\n";
        os << "        Pct time sleeping:  " << std::fixed << std::setprecision(1) << pct_time_sleeping << "%\n";
        os << "        Pct time working:   " << std::fixed << std::setprecision(1) << pct_time_working << "%\n";
        os << "\n";
    }

protected:
    virtual bool run_()
    {
        bool ran = false;
        for (auto runner : runnables_)
        {
            ran |= runner->run();
        }
        return ran;
    }

private:
    void loop_()
    {
        while (is_running_)
        {
            if (!run_())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
                ++num_times_slept_;
            }
            else
            {
                ++num_times_run_;
            }
        }

        run_();
    }

    const size_t interval_ms_;
    std::vector<pipeline::Runnable*> runnables_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> is_running_ = false;
    std::chrono::high_resolution_clock::time_point start_;
    uint64_t num_times_run_ = 0;
    uint64_t num_times_slept_ = 0;
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
    bool run_() override
    {
        bool ran = false;
        db_mgr_->safeTransaction([&]() { ran = Thread::run_(); });
        return ran;
    }

    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb::pipeline
