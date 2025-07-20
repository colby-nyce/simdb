// <CircularBuffer.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/CircularBuffer.hpp"
#include "simdb/pipeline/Task.hpp"

namespace simdb::pipeline {

template <typename DataT, size_t BufferLen>
class Task<CircularBuffer<DataT, BufferLen>> : public NonTerminalTask<DataT, DataT>
{
private:
    /// Process one item from the queue.
    bool run() override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        DataT in;
        if constexpr (std::is_arithmetic_v<DataT> && !std::is_pointer_v<DataT>)
        {
            // -Werror=maybe-uninitialized
            in = 0;
        }

        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            std::cout << "CircularBuffer::run()\n";
            std::cout << "  - New value came in: " << in << "\n";
            std::lock_guard<std::mutex> lock(mutex_);
            if (circ_buf_.full())
            {
                auto oldest = std::move(circ_buf_.pop());
                std::cout << "  - We're full. Popping and sending oldest: " << oldest;
                this->output_queue_->get().emplace(oldest);
            }
            std::cout << "  - Pushing new value into circular buffer: " << in << "\n";
            circ_buf_.push(std::move(in));
            ran = true;
        }

        return ran;
    }

    bool flushToPipeline() override
    {
        std::cout << "CircularBuffer::flushToPipeline()\n";
        bool did_work = Runnable::flushToPipeline();
        std::cout << "  - Runnable did work: " << std::boolalpha << did_work << "\n";
        std::cout << "  - Now circ_buf_.size() = " << circ_buf_.size() << "\n";

        auto send_oldest = [&]() -> bool
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!circ_buf_.empty())
            {
                auto oldest = std::move(circ_buf_.pop());
                std::cout << "  - Popping and sending oldest: " << oldest << "\n";
                this->output_queue_->get().emplace(oldest);
                return true;
            }
            return false;
        };

        while (send_oldest())
        {
            did_work = true;
        }

        return did_work;
    }

    std::string getDescription_() const override
    {
        return "CircularBuffer<" + demangle_type<DataT>() + ", " + std::to_string(BufferLen) + ">";
    }

    CircularBuffer<DataT, BufferLen> circ_buf_;
    std::mutex mutex_;
};

} // namespace simdb::pipeline
