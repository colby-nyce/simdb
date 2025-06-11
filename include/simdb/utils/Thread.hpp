#pragma once

#include <memory>
#include <thread>

namespace simdb
{

/// Base class for all threads in the SimDB library. Subclasses
/// will have their onInterval_() method called periodically based
/// on the interval given to the constructor.
class Thread
{
public:
    Thread(const size_t interval_milliseconds)
        : interval_ms_(interval_milliseconds)
    {
    }

    virtual ~Thread()
    {
        stopThreadLoop();
    }

    void startThreadLoop()
    {
        if (!is_running_)
        {
            is_running_ = true;
            thread_ = std::make_unique<std::thread>(
                [this]()
                {
                    while (is_running_)
                    {
                        onInterval_();
                        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
                    }
                });
        }
    }

    bool isRunning() const
    {
        return is_running_;
    }

    void stopThreadLoop() noexcept
    {
        if (is_running_)
        {
            is_running_ = false;
            thread_->join();
        }
    }

private:
    virtual void onInterval_() = 0;

    const size_t interval_ms_;
    std::unique_ptr<std::thread> thread_;
    bool is_running_ = false;
};

} // namespace simdb
