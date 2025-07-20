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

    Task(size_t buffer_len)
        : buffer_len_(buffer_len)
    {
        buffer_.reserve(buffer_len);
    }

    using TaskBase::getTypedInputQueue;

private:
    /// Process one item from the queue.
    bool run() override
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

        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            ran = true;
            buffer_.emplace_back(std::move(in));
            if (buffer_.size() == buffer_len_)
            {
                this->output_queue_->get().emplace(std::move(buffer_));
            }
        }

        return ran;
    }

    std::string getDescription_() const override
    {
        return "Buffer<" + demangle_type<Input>() + ">";
    }

    size_t buffer_len_;
    OutputType buffer_;
};

} // namespace simdb::pipeline
