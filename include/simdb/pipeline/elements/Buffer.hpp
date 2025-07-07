#pragma once

#include "simdb/pipeline/Task.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <vector>

namespace simdb::pipeline {

template <typename Input>
class Buffer {};

template <typename Input>
class Task<Buffer<Input>> : public NonTerminalNonDatabaseTask<Input>
{
public:
    using InputType = Input;
    using OutputType = std::vector<Input>;

    Task(size_t buffer_len)
        : buffer_len_(buffer_len)
    {
        buffer_.reserve(buffer_len);
    }

    QueueBase* getOutputQueue() override
    {
        return output_queue_;
    }

    void setOutputQueue(QueueBase* queue) override
    {
        if (auto q = dynamic_cast<Queue<OutputType>*>(queue))
        {
            output_queue_ = q;
        }
        else
        {
            throw DBException("Invalid data type");
        }
    }

    bool run() override
    {
        if (!output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        InputType in;
        bool ran = false;
        while (this->input_queue_.get().try_pop(in))
        {
            buffer_.emplace_back(std::move(in));
            if (buffer_.size() == buffer_len_)
            {
                output_queue_->get().emplace(std::move(buffer_));
                ran = true;
            }
        }

        return ran;
    }

private:
    std::string getName_() const override
    {
        return "Buffer<" + demangle_type<Input>() + ">";
    }

    size_t buffer_len_;
    OutputType buffer_;
    Queue<OutputType>* output_queue_ = nullptr;
};

} // namespace simdb::pipeline
