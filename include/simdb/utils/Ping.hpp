// <Ping.hpp> -*- C++ -*-

#pragma once

#include <chrono>
#include <functional>
#include <thread>

namespace simdb
{

/// This utility class is used to "wake up" a boolean flag every N seconds.
class Ping
{
public:
    Ping(double timeout_seconds = 1.0)
        : timeout_ms_(timeout_seconds * 1000)
    {
    }

    ~Ping()
    {
        teardown();
    }

    operator bool()
    {
        if (!continue_)
            return true;
        auto ready = ready_;
        ready_ = false;
        return ready;
    }

    void teardown() noexcept
    {
        continue_ = false;
        if (ping_thread_.joinable())
        {
            ping_thread_.join();
        }
    }

private:
    void makeReady_()
    {
        while (continue_)
        {
            ready_ = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms_));
        }
    }

    bool ready_ = false;
    bool continue_ = true;
    uint32_t timeout_ms_;
    std::thread ping_thread_{std::bind(&Ping::makeReady_, this)};
};

} // namespace simdb
