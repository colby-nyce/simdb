// <CircularBuffer.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/CircularBuffer.hpp"
#include "simdb/pipeline/Task.hpp"

namespace simdb::pipeline {

template <typename DataT, size_t BufferLen>
class Task<CircularBuffer<DataT, BufferLen>> : public NonTerminalTask<DataT, DataT>
{
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
            if (circ_buf_.full())
            {
                this->output_queue_->get().emplace(std::move(circ_buf_.pop()));
            }
            circ_buf_.push(std::move(in));
            ran = true;
        }

        return ran;
    }

    bool flushToPipeline() override
    {
        bool did_work = Runnable::flushToPipeline();

        while (!circ_buf_.empty())
        {
            this->output_queue_->get().emplace(std::move(circ_buf_.pop()));
            did_work = true;
        }

        return did_work;
    }

private:
    std::string getDescription_() const override
    {
        return "CircularBuffer<" + demangle_type<DataT>() + ", " + std::to_string(BufferLen) + ">";
    }

    CircularBuffer<DataT, BufferLen> circ_buf_;
};

} // namespace simdb::pipeline
