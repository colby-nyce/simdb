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
template <typename T>
inline void compressDataVec(const std::vector<T>& in, std::vector<char>& out, CompressionLevel compression_level = CompressionLevel::DEFAULT)
{
    if (in.empty())
    {
        out.clear();
        return;
    }

    z_stream defstream{};
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;

    auto num_bytes_before = in.size() * sizeof(T);
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
    out.resize(num_bytes_after);
}

/// Perform zlib decompression.
template <typename T>
inline void decompressDataVec(const std::vector<char>& in, std::vector<T>& out)
{
    if (in.empty()) {
        out.clear();
        return;
    }

    constexpr size_t CHUNK_SIZE = 1024 * 64;
    std::vector<char> decompressed_buffer;
    decompressed_buffer.reserve(CHUNK_SIZE);

    z_stream stream{};
    stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());

    if (inflateInit(&stream) != Z_OK)
    {
        throw DBException("Failed to initialize zlib inflate stream.");
    }

    int ret;
    do
    {
        size_t current_size = decompressed_buffer.size();
        decompressed_buffer.resize(current_size + CHUNK_SIZE);
        stream.next_out = reinterpret_cast<Bytef*>(&decompressed_buffer[current_size]);
        stream.avail_out = CHUNK_SIZE;

        ret = inflate(&stream, Z_NO_FLUSH);

        if (ret != Z_OK && ret != Z_STREAM_END)
        {
            inflateEnd(&stream);
            throw DBException("Decompression failed with zlib error code: " + std::to_string(ret));
        }

        // Adjust actual used size
        decompressed_buffer.resize(current_size + (CHUNK_SIZE - stream.avail_out));
    } while (ret != Z_STREAM_END);

    inflateEnd(&stream);

    // Convert decompressed bytes to std::vector<T>
    size_t byte_count = decompressed_buffer.size();
    if (byte_count % sizeof(T) != 0)
    {
        throw DBException("Decompressed data size is not aligned with type T.");
    }

    size_t elem_count = byte_count / sizeof(T);
    out.resize(elem_count);
    std::memcpy(out.data(), decompressed_buffer.data(), byte_count);
}

} // namespace simdb
