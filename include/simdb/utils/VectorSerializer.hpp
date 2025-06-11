#pragma once

#include "simdb/Exceptions.hpp"
#include <vector>

namespace simdb
{

/// Serializes a vector of data into a byte array.
template <typename T>
class VectorSerializer
{
public:
    VectorSerializer(std::vector<char>&& out, const std::vector<T>* initial_data = nullptr)
        : output_(std::move(out))
    {
        if (initial_data)
        {
            *this = *initial_data;
        }
        else
        {
            output_.clear();
        }
    }

    std::vector<char> release()
    {
        return std::move(output_);
    }

    T& operator[](size_t idx)
    {
        char* buf = output_.data() + idx * sizeof(T);
        return *reinterpret_cast<T*>(buf);
    }

    T& operator[](size_t idx) const
    {
        const char* buf = output_.data() + idx * sizeof(T);
        return *reinterpret_cast<const T*>(buf);
    }

    T& at(size_t idx)
    {
        if (idx * sizeof(T) >= output_.size())
        {
            throw DBException("Index out of range in VectorSerializer");
        }
        return (*this)[idx];
    }

    T& at(size_t idx) const
    {
        if (idx * sizeof(T) >= output_.size())
        {
            throw DBException("Index out of range in VectorSerializer");
        }
        return (*this)[idx];
    }

    template <typename tt = T>
    typename std::enable_if<(sizeof(tt) <= sizeof(uint64_t)), void>::type
    push_back(const T value)
    {
        size_t idx = output_.size() / sizeof(T);
        output_.resize(output_.size() + sizeof(T));
        (*this)[idx] = value;
    }

    template <typename tt = T>
    typename std::enable_if<(sizeof(tt) > sizeof(uint64_t)), void>::type
    push_back(const T& value)
    {
        size_t idx = output_.size() / sizeof(T);
        output_.resize(output_.size() + sizeof(T));
        (*this)[idx] = value;
    }

    VectorSerializer& operator=(const std::vector<T>& vec)
    {
        output_.resize(vec.size() * sizeof(T));
        memcpy(output_.data(), vec.data(), output_.size());
        return *this;
    }

private:
    std::vector<char> output_;
};

} // namespace simdb
