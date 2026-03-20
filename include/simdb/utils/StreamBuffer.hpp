// <StreamBuffer.hpp> -*- C++ -*-

#pragma once

#include <array>
#include <cstring>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace simdb {

/// \class StreamBuffer
/// \brief Utility class which wraps a char buffer with ostream operators.
class StreamBuffer
{
public:
    /// Create with a reference to the final buffer.
    explicit StreamBuffer(std::vector<char> & out) :
        out_(out)
    {
        out_.clear();
    }

    void append(const void * data, const size_t num_bytes)
    {
        const auto * bytes = static_cast<const char*>(data);
        out_.insert(out_.end(), bytes, bytes + num_bytes);
    }

    using bool_type = uint8_t;

    bool operator==(const StreamBuffer & other) const
    {
        return out_ == other.out_;
    }

    bool operator==(const std::vector<char> & other) const
    {
        return out_ == other;
    }

private:
    std::vector<char> & out_;
};

inline StreamBuffer & operator<<(StreamBuffer & buf, const std::string & val)
{
    buf.append(val.data(), val.size());
    return buf;
}

inline StreamBuffer & operator<<(StreamBuffer & buf, const char * val)
{
    if (val != nullptr)
    {
        buf.append(val, std::strlen(val));
    }
    return buf;
}

inline StreamBuffer & operator<<(StreamBuffer & buf, const bool val)
{
    using bool_type = typename StreamBuffer::bool_type;
    const bool_type byte = val ? bool_type{1} : bool_type{0};
    buf.append(&byte, sizeof(byte));
    return buf;
}

template <typename T>
constexpr bool is_stream_array_value_type_v =
    (std::is_scalar_v<T> &&
     std::is_trivially_copyable_v<T> &&
     std::is_standard_layout_v<T> &&
     !std::is_enum_v<T> &&
     !std::is_same_v<T, bool>) ||
    std::is_enum_v<T> ||
    std::is_same_v<T, bool>;

template <typename T, size_t N>
StreamBuffer & operator<<(StreamBuffer & buf, const T (&val)[N])
{
    using ValueType = std::remove_cv_t<T>;
    static_assert(
        is_stream_array_value_type_v<ValueType>,
        "StreamBuffer array operator<< requires scalar POD, scalar enum, or scalar bool value types");

    for (const auto & elem : val)
    {
        buf << elem;
    }
    return buf;
}

template <typename T, size_t N>
StreamBuffer & operator<<(StreamBuffer & buf, const std::array<T, N> & val)
{
    using ValueType = std::remove_cv_t<T>;
    static_assert(
        is_stream_array_value_type_v<ValueType>,
        "StreamBuffer std::array operator<< requires scalar POD, scalar enum, or scalar bool value types");

    for (const auto & elem : val)
    {
        buf << elem;
    }
    return buf;
}

template <
    typename T,
    typename ValueType = std::remove_cv_t<std::remove_reference_t<T>>,
    std::enable_if_t<!std::is_enum_v<ValueType> && !std::is_same_v<ValueType, bool>, int> = 0>
StreamBuffer & operator<<(StreamBuffer & buf, const T & val)
{
    static_assert(
        std::is_scalar_v<ValueType> &&
        std::is_trivially_copyable_v<ValueType> &&
        std::is_standard_layout_v<ValueType> &&
        "StreamBuffer::operator<< requires scalar POD-like types");
    buf.append(&val, sizeof(T));
    return buf;
}

template <
    typename T,
    typename ValueType = std::remove_cv_t<std::remove_reference_t<T>>,
    std::enable_if_t<std::is_enum_v<ValueType>, int> = 0>
StreamBuffer & operator<<(StreamBuffer & buf, const T & val)
{
    using Underlying = std::underlying_type_t<ValueType>;
    static_assert(
        std::is_scalar_v<Underlying> &&
        std::is_trivially_copyable_v<Underlying> &&
        std::is_standard_layout_v<Underlying> &&
        !std::is_same_v<Underlying, bool>,
        "StreamBuffer enum operator<< requires a scalar underlying type (excluding bool)");

    const auto underlying = static_cast<Underlying>(val);
    buf.append(&underlying, sizeof(Underlying));
    return buf;
}

} // namespace simdb
