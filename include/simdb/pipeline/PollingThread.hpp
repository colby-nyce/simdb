// <PollingThread.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Runnable.hpp"
#include "simdb/Exceptions.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
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
        runnable->setPollingThread_(this);
        runnables_.emplace_back(runnable);
    }

    virtual bool flushRunnables()
    {
        bool did_work = false;
        for (auto runnable : runnables_)
        {
            if (!runnable->enabled())
            {
                continue;
            }

            if (runnable->processAll(true) == RunnableOutcome::DID_WORK)
            {
                did_work = true;
            }
        }
        return did_work;
    }

    virtual void open()
    {
        if (runnables_.empty())
        {
            return;
        }

        if (!thread_)
        {
            stop_requested_ = false;
            paused_ = false;
            is_running_ = true;
            start_ = std::chrono::high_resolution_clock::now();
            thread_ = std::make_unique<std::thread>(&PollingThread::loop_, this);
        }
    }

    virtual void close() noexcept
    {
        if (!thread_)
        {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(pause_mutex_);
            stop_requested_ = true;
            paused_ = false; // Unpause in case it was paused
        }
        pause_cv_.notify_all();

        if (thread_->joinable())
        {
            thread_->join();
        }
        is_running_ = false;
        thread_.reset();
    }

    void pause()
    {
        if (!is_running_ || paused_)
        {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(pause_mutex_);
            paused_ = true;

            // Reset and prepare for acknowledgment
            paused_promise_ = std::promise<void>();
            has_pending_pause_ack_ = true;
        }

        pause_cv_.notify_all(); // Wake the thread so it can acknowledge

        // Wait for thread to acknowledge it is paused
        paused_promise_.get_future().wait();
    }

    bool paused()
    {
        if (!is_running_)
        {
            throw DBException("Cannot check paused state of a non-running thread");
        }
        return paused_;
    }

    void resume()
    {
        if (!is_running_ || !paused_)
        {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(pause_mutex_);
            paused_ = false;
        }

        pause_cv_.notify_all(); // Wake the thread to resume
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
        const auto total_elap_seconds = dur.count();
        const auto pct_time_sleeping = (total_sleep_seconds_ / total_elap_seconds) * 100;
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
        while (!stop_requested_)
        {
            // Pause handling
            {
                std::unique_lock<std::mutex> lock(pause_mutex_);
                while (paused_ && !stop_requested_)
                {
                    if (has_pending_pause_ack_)
                    {
                        paused_promise_.set_value();
                        has_pending_pause_ack_ = false;
                    }
                    pause_cv_.wait(lock);
                }

                if (stop_requested_)
                {
                    break;
                }
            }

            if (!run_(false))
            {
                // Sleep for a fixed amount of time before polling all runnables again
                // but wake early if paused or stop is requested
                auto sleep_start = std::chrono::high_resolution_clock::now();
                std::unique_lock<std::mutex> lock(pause_mutex_);
                pause_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms_), [this] {
                    return paused_ || stop_requested_;
                });

                auto sleep_end = std::chrono::high_resolution_clock::now();
                auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(sleep_end - sleep_start);
                total_sleep_seconds_ += duration_us.count() / 1'000'000.0;
            }
            else
            {
                ++num_times_run_;
            }
        }

        // Flush
        while (run_(true)) {}
    }

    virtual bool run_(bool force)
    {
        bool did_work = false;
        while (true)
        {
            bool processed = false;
            for (auto runner : runnables_)
            {
                if (!runner->enabled())
                {
                    continue;
                }

                if (runner->processOne(force) == RunnableOutcome::DID_WORK)
                {
                    processed = true;
                }
            }
            if (!processed)
            {
                break;
            }
            did_work = true;
        }
        return did_work;
    }

    const size_t interval_ms_;
    std::vector<Runnable*> runnables_;
    std::unique_ptr<std::thread> thread_;

    std::mutex pause_mutex_;
    std::condition_variable pause_cv_;

    std::atomic<bool> is_running_ = false;
    std::atomic<bool> paused_ = false;
    std::atomic<bool> stop_requested_ = false;

    std::promise<void> paused_promise_;
    bool has_pending_pause_ack_ = false;

    std::chrono::high_resolution_clock::time_point start_;
    uint64_t num_times_run_ = 0;
    double total_sleep_seconds_ = 0;
};

/// Defined here so we can avoid circular includes
inline ScopedRunnableDisabler::ScopedRunnableDisabler(
    const std::vector<Runnable*>& runnables,
    const std::vector<PollingThread*>& polling_threads)
{
    for (auto pt : polling_threads)
    {
        if (!pt->paused())
        {
            pt->pause();
            paused_threads_.push_back(pt);
        }
    }

    for (auto r : runnables)
    {
        if (r->enabled())
        {
            r->enable(false);
            disabled_runnables_.push_back(r);
        }
    }
}

/// Defined here so we can avoid circular includes
inline ScopedRunnableDisabler::~ScopedRunnableDisabler()
{
    for (auto pt : paused_threads_)
    {
        pt->resume();
    }

    for (auto r : disabled_runnables_)
    {
        r->enable(true);
    }
}

} // namespace simdb::pipeline
