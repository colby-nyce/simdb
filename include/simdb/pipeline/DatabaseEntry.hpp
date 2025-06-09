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

using ReroutedCallback = std::function<void(DatabaseManager*)>;
using EndOfPipelineCallback = std::function<void(DatabaseEntry&&)>;

#define END_OF_PIPELINE_CALLBACK(Class, Method) \
    std::bind(&Class::Method, dynamic_cast<Class*>(__this__), std::placeholders::_1)

class DatabaseEntry
{
public:
    template <typename T>
    DatabaseEntry(uint64_t tick, const std::vector<T>& data, DatabaseManager* db_mgr,
                  bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr)
    {
        accept_(data);
    }

    template <typename T, size_t N>
    DatabaseEntry(uint64_t tick, const std::array<T,N>& data, DatabaseManager* db_mgr,
                  bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr)
    {
        accept_(data);
    }

    template <typename T>
    DatabaseEntry(uint64_t tick, std::vector<T>&& data, DatabaseManager* db_mgr,
                  bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr)
    {
        accept_(std::move(data));
    }

    template <typename T, size_t N>
    DatabaseEntry(uint64_t tick, std::array<T,N>&& data, DatabaseManager* db_mgr,
                  bool already_compressed = false)
        : DatabaseEntry(tick, db_mgr)
    {
        accept_(std::move(data));
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

    void redirect(EndOfPipelineCallback end_of_pipeline_callback)
    {
        end_of_pipeline_callback_ = end_of_pipeline_callback;
        rerouted_callback_ = nullptr;
    }

    void redirect(ReroutedCallback rerouted_callback)
    {
        rerouted_callback_ = rerouted_callback;
        end_of_pipeline_callback_ = nullptr;
    }

    void setOnCommitCallback(std::function<void(const int datablob_db_id, const uint64_t tick)> callback) {
        on_commit_callback_ = callback;
    }

    EndOfPipelineCallback getEndOfPipelineCallback() const
    {
        return end_of_pipeline_callback_;
    }

    ReroutedCallback getReroutedCallback() const
    {
        return rerouted_callback_;
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
        accept_(std::move(compressed_data));

        compressed_ = true;
    }

    void onCommit(const int datablob_db_id)
    {
        if (on_commit_callback_)
        {
            on_commit_callback_(datablob_db_id, tick_);
        }
    }

private:
    DatabaseEntry(uint64_t tick, DatabaseManager* db_mgr, bool already_compressed = false)
        : tick_(tick)
        , compressed_(already_compressed)
        , db_mgr_(db_mgr)
    {
    }

    template <typename T>
    void accept_(std::vector<T>&& data)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = std::move(data);
    }

    template <typename T, size_t N>
    void accept_(std::array<T, N>&& data)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = std::move(data);
    }

    template <typename T>
    void accept_(const std::vector<T>& data)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = data;
    }

    template <typename T, size_t N>
    void accept_(const std::array<T, N>& data)
    {
        data_ptr_ = data.data();
        num_bytes_ = data.size() * sizeof(T);
        container_ = data;
    }

    uint64_t tick_ = 0;
    const void* data_ptr_ = nullptr;
    size_t num_bytes_ = 0;
    bool compressed_ = false;
    DatabaseManager* db_mgr_ = nullptr;

    // This can hold any type of contiguous data, such as
    // std::vector<char> or std::array<T, N>.
    std::any container_;

    // By default, the DatabaseThread will send the data entry
    // to the user-provided callback to handle the data. Some
    // users may want to selectively override this behavior
    // and reroute the data to a different callback. If provided,
    // this callback will be called with the DatabaseManager
    // that was originally set in the DatabaseEntry.
    ReroutedCallback rerouted_callback_ = nullptr;

    // Allow applications to set the end-of-pipeline callback.
    EndOfPipelineCallback end_of_pipeline_callback_ = nullptr;

    // Let users react to the commit of this entry.
    std::function<void(const int datablob_db_id, const uint64_t tick)> on_commit_callback_;
};

} // namespace simdb
