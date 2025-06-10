#pragma once

#include "simdb/utils/Compress.hpp"
#include "simdb/schema/Blob.hpp"

#include <any>
#include <array>
#include <functional>
#include <vector>

namespace simdb
{

class DatabaseEntry;
class DatabaseManager;

using EndOfPipelineCallback = std::function<void(const DatabaseEntry&)>;

#define END_OF_PIPELINE_CALLBACK(Class, Method) \
    std::bind(&Class::Method, dynamic_cast<Class*>(__this__), std::placeholders::_1)

class DatabaseEntry
{
public:
    template <typename T>
    DatabaseEntry(uint64_t tick, const std::vector<T>& data, DatabaseManager* db_mgr,
                  const void* committer, bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr, committer)
    {
        accept_(data, already_compressed);
    }

    template <typename T, size_t N>
    DatabaseEntry(uint64_t tick, const std::array<T,N>& data, DatabaseManager* db_mgr,
                  const void* committer, bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr, committer)
    {
        accept_(data, already_compressed);
    }

    template <typename T>
    DatabaseEntry(uint64_t tick, std::vector<T>&& data, DatabaseManager* db_mgr,
                  const void* committer, bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr, committer)
    {
        accept_(std::move(data), already_compressed);
    }

    template <typename T, size_t N>
    DatabaseEntry(uint64_t tick, std::array<T,N>&& data, DatabaseManager* db_mgr,
                  const void* committer, bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr, committer)
    {
        accept_(std::move(data), already_compressed);
    }

    DatabaseEntry() = default;

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    uint64_t getTick() const
    {
        return tick_;
    }

    SqlBlob getBlob() const
    {
        SqlBlob blob;
        blob.data_ptr = data_ptr_;
        blob.num_bytes = num_bytes_;
        return blob;
    }

    const void* getCommitter() const
    {
        return committer_;
    }

    void setEndOfPipelineCallback(EndOfPipelineCallback callback)
    {
        end_of_pipeline_callback_ = callback;
    }

    bool compressed() const
    {
        return compressed_;
    }

    void compress()
    {
        if (compressed_ || data_ptr_ == nullptr || num_bytes_ == 0)
        {
            return;
        }

        std::vector<char> compressed_data;
        compressDataVec(data_ptr_, num_bytes_, compressed_data);
        accept_(std::move(compressed_data), true);
    }

    void retire()
    {
        if (end_of_pipeline_callback_)
        {
            end_of_pipeline_callback_(*this);
        }
    }

private:
    DatabaseEntry(uint64_t tick, DatabaseManager* db_mgr, const void* committer)
        : tick_(tick)
        , db_mgr_(db_mgr)
        , committer_(committer)
    {
    }

    template <typename T>
    void accept_(std::vector<T>&& data, bool already_compressed)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = std::move(data);
        compressed_ = already_compressed;
    }

    template <typename T, size_t N>
    void accept_(std::array<T, N>&& data, bool already_compressed)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = std::move(data);
        compressed_ = already_compressed;
    }

    template <typename T>
    void accept_(const std::vector<T>& data, bool already_compressed)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = data;
        compressed_ = already_compressed;
    }

    template <typename T, size_t N>
    void accept_(const std::array<T, N>& data, bool already_compressed)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = data;
        compressed_ = already_compressed;
    }

    uint64_t tick_ = 0;
    const void* data_ptr_ = nullptr;
    size_t num_bytes_ = 0;
    bool compressed_ = false;
    DatabaseManager* db_mgr_ = nullptr;
    const void* committer_ = nullptr;

    // This can hold any type of contiguous data, such as
    // std::vector<char> or std::array<T, N>.
    std::any container_;

    // Allow applications to set the end-of-pipeline callback.
    EndOfPipelineCallback end_of_pipeline_callback_ = nullptr;
};

} // namespace simdb
