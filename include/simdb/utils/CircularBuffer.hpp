// <CircularBuffer.hpp> -*- C++ -*-

#pragma once

#include "simdb/Exceptions.hpp"
#include <array>
#include <cstddef>

namespace simdb {

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

} // namespace simdb
