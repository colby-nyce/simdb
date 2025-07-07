#pragma once

#include "simdb/pipeline/Task.hpp"
#include <array>

template <typename DataT, size_t BufferLen>
class CircularBuffer
{
public:
    // Add an element using move semantics
    void push(DataT&& item)
    {
        array_[head_] = std::move(item);
        advanceHead_();
    }

    // Emplace construct in-place
    template <typename... Args>
    void emplace(Args&&... args)
    {
        array_[head_] = DataT(std::forward<Args>(args)...);
        advanceHead_();
    }

    // Pop the oldest element (moved out)
    DataT pop()
    {
        if (empty())
        {
            throw simdb::DBException("Buffer is empty");
        }

        DataT item = std::move(array_[tail_]);
        full_ = false;
        tail_ = (tail_ + 1) % BufferLen;
        return item;
    }

    // Check if empty
    bool empty() const
    {
        return !full_ && head_ == tail_;
    }

    // Check if full
    bool full() const
    {
        return full_;
    }

    // Get number of elements in buffer
    size_t size() const
    {
        if (full_)
        {
            return BufferLen;
        }

        if (head_ >= tail_)
        {
            return head_ - tail_;
        }

        return BufferLen + head_ - tail_;
    }

    // Reset the buffer (does not deallocate DataT's - based on std::array)
    void reset()
    {
        head_ = tail_;
        full_ = false;
    }

private:
    void advanceHead_()
    {
        if (full_)
        {
            tail_ = (tail_ + 1) % BufferLen;
        }
        head_ = (head_ + 1) % BufferLen;
        full_ = (head_ == tail_);
    }

    std::array<DataT, BufferLen> array_;
    size_t head_ = 0;
    size_t tail_ = 0;
    bool full_ = false;
};

namespace simdb::pipeline {

template <typename DataT, size_t BufferLen>
class Task<CircularBuffer<DataT, BufferLen>> : public TaskBase
{
public:
    using InputType = DataT;
    using OutputType = DataT;

    QueueBase* getInputQueue() override
    {
        return &input_queue_;
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

    bool requiresDatabase() const override
    {
        return false;
    }

    bool run() override
    {
        InputType in;
        bool ran = false;
        while (input_queue_.get().try_pop(in))
        {
            if (circ_buf_.full())
            {
                output_queue_->get().emplace(std::move(circ_buf_.pop()));
                ran = true;
            }
            circ_buf_.push(std::move(in));
        }

        return ran;
    }

private:
    std::string getName_() const override
    {
        return "CircularBuffer<" + demangle_type<DataT>() + ", " + std::to_string(BufferLen) + ">";
    }

    CircularBuffer<InputType, BufferLen> circ_buf_;
    Queue<InputType> input_queue_;
    Queue<OutputType>* output_queue_ = nullptr;
};

} // namespace simdb::pipeline
