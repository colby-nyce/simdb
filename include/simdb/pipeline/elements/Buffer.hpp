#pragma once

#include "simdb/pipeline/Task.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <vector>

namespace simdb::pipeline {

template <typename Input>
class Buffer
{
public:
    using InputType = Input;
    using OutputType = std::vector<Input>;
};

template <typename Input>
class Task<Buffer<Input>> : public NonTerminalTask<Input, typename Buffer<Input>::OutputType>
{
public:
    using InputType = Input;
    using OutputType = std::vector<Input>;

    Task(size_t buffer_len, bool flush_partial=false)
        : buffer_len_(buffer_len)
        , flush_partial_(flush_partial)
    {
        buffer_.reserve(buffer_len);
    }

    using TaskBase::getTypedInputQueue;

private:
    /// Process one item from the queue.
    RunnableOutcome processOne(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        InputType in;
        if constexpr (std::is_arithmetic_v<InputType> && !std::is_pointer_v<InputType>)
        {
            // -Werror=maybe-uninitialized
            in = 0;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        if (this->input_queue_->get().try_pop(in))
        {
            outcome = RunnableOutcome::DID_WORK;
            buffer_.emplace_back(std::move(in));
            if (buffer_.size() == buffer_len_)
            {
                this->output_queue_->get().emplace(std::move(buffer_));
            }
        }

        return outcome;
    }

    /// Process all items from the queue.
    RunnableOutcome processAll(bool force) override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        InputType in;
        if constexpr (std::is_arithmetic_v<InputType> && !std::is_pointer_v<InputType>)
        {
            // -Werror=maybe-uninitialized
            in = 0;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        RunnableOutcome outcome = RunnableOutcome::NO_OP;
        while (this->input_queue_->get().try_pop(in))
        {
            outcome = RunnableOutcome::DID_WORK;
            buffer_.emplace_back(std::move(in));
            if (buffer_.size() == buffer_len_)
            {
                this->output_queue_->get().emplace(std::move(buffer_));
            }
        }

        if (force && (full_() || (!empty_() && flush_partial_)))
        {
            this->output_queue_->get().emplace(std::move(buffer_));
            outcome = RunnableOutcome::DID_WORK;
        }

        return outcome;
    }

    bool full_() const
    {
        return buffer_.size() == buffer_len_;
    }

    bool empty_() const
    {
        return buffer_.empty();
    }

    std::string getDescription_() const override
    {
        return "Buffer<" + demangle_type<Input>() + ">";
    }

    size_t buffer_len_;
    OutputType buffer_;
    bool flush_partial_;

    /// Mutex to protect the buffer
    std::mutex mutex_;
};

} // namespace simdb::pipeline
