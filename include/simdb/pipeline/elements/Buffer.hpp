#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Demangle.hpp"
#include <vector>

namespace simdb::pipeline {

/// Buffer task element.
template <typename Input>
class Buffer
{
public:
    using InputType = Input;
    using OutputType = std::vector<Input>;

    Buffer(size_t buffer_len)
        : buffer_len_(buffer_len)
    {
        buffer_.reserve(buffer_len);
    }

    std::string getName() const
    {
        return "Buffer<" + demangle_type<Input>() + ">";
    }

    bool operator()(InputType&& in, ConcurrentQueue<OutputType>& out)
    {
        buffer_.emplace_back(std::move(in));
        if (buffer_.size() == buffer_len_)
        {
            out.emplace(std::move(buffer_));
            return true;
        }
        return false;
    }

private:
    size_t buffer_len_;
    std::vector<Input> buffer_;
};

} // namespace simdb::pipeline
