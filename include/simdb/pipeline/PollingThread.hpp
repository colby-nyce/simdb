// <PollingThread.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Runnable.hpp"
#include "simdb/Exceptions.hpp"

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

namespace simdb::pipeline {

/// Timer thread which "polls" its runnables for any activity. Goes back
/// to sleep for a fixed amount of time before polling again.
class PollingThread
{
public:
    /// Create a thread with an "interval" in milliseconds. This value says
    /// how long the thread should sleep if none of its Runnables had any
    /// work to do.
    PollingThread(const size_t interval_milliseconds = 100)
        : interval_ms_(interval_milliseconds)
    {
    }

    virtual ~PollingThread() noexcept = default;

    void addRunnable(Runnable* runnable)
    {
        if (is_running_)
        {
            throw DBException("Cannot add runnables while thread is running");
        }
        runnables_.emplace_back(runnable);
    }

    virtual void open()
    {
        if (runnables_.empty())
        {
            return;
        }

        if (!is_running_)
        {
            is_running_ = true;
            start_ = std::chrono::high_resolution_clock::now();
            thread_ = std::make_unique<std::thread>(&PollingThread::loop_, this);
        }
    }

    virtual void close() noexcept
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
        if (runnables_.empty())
        {
            return;
        }

        if (is_running_)
        {
            return;
        }

        auto now = std::chrono::high_resolution_clock::now();
        const std::chrono::duration<double> dur = now - start_;
        const double total_sleep_seconds = 1.0 * num_times_slept_ * interval_ms_ / 1000.0;
        const auto total_elap_seconds = dur.count();
        const auto pct_time_sleeping = (total_sleep_seconds / total_elap_seconds) * 100;
        const auto pct_time_working = 100 - pct_time_sleeping;

        os << "Thread containing:\n";
        for (const auto runnable : runnables_)
        {
            runnable->print(os, 4);
        }

        os << "\n";
        os << "    Performance report:\n";
        os << "        Num times run:      " << num_times_run_ << "\n";
        os << "        Pct time sleeping:  " << std::fixed << std::setprecision(1) << pct_time_sleeping << "%\n";
        os << "        Pct time working:   " << std::fixed << std::setprecision(1) << pct_time_working << "%\n";
        os << "\n";
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

    virtual bool run_()
    {
        bool ran = false;
        for (auto runner : runnables_)
        {
            ran |= runner->run();
        }
        return ran;
    }

    const size_t interval_ms_;
    std::vector<Runnable*> runnables_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> is_running_ = false;
    std::chrono::high_resolution_clock::time_point start_;
    uint64_t num_times_run_ = 0;
    uint64_t num_times_slept_ = 0;
};

} // namespace simdb::pipeline
