// <Compress.hpp> -*- C++ -*-

#pragma once

#include <zlib.h>
#include <vector>
#include <cstring>
#include "simdb/Exceptions.hpp"

namespace simdb
{

/// Compression modes of operation.
enum class CompressionModes
{
    COMPRESSED,
    UNCOMPRESSED
};

/// Compression levels for zlib.
enum class CompressionLevel
{
    DISABLED = Z_NO_COMPRESSION,
    DEFAULT  = Z_DEFAULT_COMPRESSION,
    FASTEST  = Z_BEST_SPEED,
    HIGHEST  = Z_BEST_COMPRESSION
};

/// Perform zlib compression.
template <typename InVectorT, typename OutVectorT>
inline void compressDataVec(const std::vector<InVectorT>& in,
                            std::vector<OutVectorT>& out,
                            CompressionLevel compression_level = CompressionLevel::DEFAULT)
{
    static_assert(std::is_trivial_v<InVectorT> && std::is_trivial_v<OutVectorT>,
                  "InVectorT and OutVectorT must be trivial types for zlib compression.");

    static_assert(std::is_standard_layout_v<InVectorT> && std::is_standard_layout_v<OutVectorT>,
                  "InVectorT and OutVectorT must be standard layout types for zlib compression.");

    if (in.empty())
    {
        out.clear();
        return;
    }

    z_stream defstream{};
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;

    auto num_bytes_before = in.size() * sizeof(InVectorT);
    defstream.avail_in = (uInt)(num_bytes_before);
    defstream.next_in = (Bytef*)(in.data());

    // Compression can technically result in a larger output, although it is not
    // likely except for possibly very small input vectors. There is no deterministic
    // value for the maximum number of bytes after decompression, but we can choose
    // a very safe minimum.
    auto max_bytes_after = num_bytes_before * 2;
    if (max_bytes_after < 1000)
    {
        max_bytes_after = 1000;
    }
    out.resize(max_bytes_after);

    defstream.avail_out = (uInt)(out.size());
    defstream.next_out = (Bytef*)(out.data());

    deflateInit(&defstream, static_cast<int>(compression_level));
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);

    auto num_bytes_after = (int)defstream.total_out;
    out.resize(num_bytes_after / sizeof(OutVectorT));
}

/// Perform zlib decompression.
template <typename InVectorT, typename OutVectorT>
inline void decompressDataVec(const std::vector<InVectorT>& in, std::vector<OutVectorT>& out)
{
    static_assert(std::is_trivial_v<InVectorT> && std::is_trivial_v<OutVectorT>,
                  "InVectorT and OutVectorT must be trivial types for zlib compression.");

    static_assert(std::is_standard_layout_v<InVectorT> && std::is_standard_layout_v<OutVectorT>,
                  "InVectorT and OutVectorT must be standard layout types for zlib compression.");

    if (in.empty()) {
        out.clear();
        return;
    }

    constexpr size_t CHUNK_SIZE = 1024 * 64;
    out.clear();
    out.reserve(CHUNK_SIZE); // Reserve space for decompressed data

    z_stream stream{};
    stream.next_in = (Bytef*)(in.data());
    stream.avail_in = static_cast<uInt>(in.size() * sizeof(InVectorT));

    if (inflateInit(&stream) != Z_OK)
    {
        throw DBException("Failed to initialize zlib inflate stream.");
    }

    do
    {
        size_t current_size = out.size();
        out.resize(current_size + CHUNK_SIZE);
        stream.next_out = (Bytef*)(&out[current_size]);
        stream.avail_out = CHUNK_SIZE * sizeof(OutVectorT);

        int ret = inflate(&stream, Z_NO_FLUSH);

        if (ret != Z_OK && ret != Z_STREAM_END)
        {
            inflateEnd(&stream);
            throw DBException("Decompression failed with zlib error code: " + std::to_string(ret));
        }
    } while (stream.avail_out == 0);

    // Adjust actual used size
    size_t current_size = out.size();
    out.resize(current_size - stream.avail_out / sizeof(OutVectorT));
    inflateEnd(&stream);
}

} // namespace simdb
