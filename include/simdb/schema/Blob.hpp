// <Blob.hpp> -*- C++ -*-

#pragma once

#include <cstddef>
#include <iostream>
#include <vector>

namespace simdb {

/// Blob descriptor used for writing and reading raw bytes
/// to/from the database.
struct SqlBlob
{
    const void* data_ptr = nullptr;
    size_t num_bytes = 0;

    template <typename T>
    SqlBlob(const std::vector<T>& vals) : data_ptr(vals.data()), num_bytes(vals.size() * sizeof(T))
    {
    }

    SqlBlob() = default;
    SqlBlob(const SqlBlob&) = default;
};

inline std::ostream& operator<<(std::ostream& os, const SqlBlob& blob)
{
    if (!blob.data_ptr || blob.num_bytes == 0)
    {
        os << "blob(empty)";
        return os;
    }

    os << "blob(" << std::hex << reinterpret_cast<uintptr_t>(blob.data_ptr) << ", n=" << std::dec << blob.num_bytes
       << ")";

    return os;
}

} // namespace simdb
