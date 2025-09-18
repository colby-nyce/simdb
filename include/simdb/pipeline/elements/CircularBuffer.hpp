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
    bool run(bool force_flush) override
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
            std::lock_guard<std::mutex> lock(mutex_);
            if (circ_buf_.full())
            {
                auto oldest = std::move(circ_buf_.pop());
                this->output_queue_->get().emplace(std::move(oldest));
            }
            circ_buf_.push(std::move(in));
            ran = true;
        }

        if (force_flush)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            while (!circ_buf_.empty())
            {
                auto oldest = std::move(circ_buf_.pop());
                this->output_queue_->get().emplace(std::move(oldest));
                ran = true;
            }
        }

        return ran;
    }

    bool flushToPipeline() override
    {
        bool did_work = Runnable::flushToPipeline();

        auto send_oldest = [&]() -> bool
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!circ_buf_.empty())
            {
                auto oldest = std::move(circ_buf_.pop());
                this->output_queue_->get().emplace(std::move(oldest));
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
