#pragma once

#include "simdb/pipeline/PipelineChain.hpp"
#include "simdb/utils/MetaStructs.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/schema/Blob.hpp"

namespace simdb
{

class App;
class DatabaseManager;
class PipelineChainLink;
class PipelineStage;
class Pipeline;

/// Represents a single entry in a processing pipeline.
class PipelineEntry
{
public:
    PipelineEntry(uint64_t tick, DatabaseManager* db_mgr, std::vector<char>&& bytes)
        : tick_(tick)
        , db_mgr_(db_mgr)
        , bytes_(std::move(bytes))
    {
    }

    PipelineEntry() = default;

    virtual ~PipelineEntry() = default;

    void setOwningApp(App* app)
    {
        owning_app_ = app;
    }

    App* getOwningApp() const
    {
        return owning_app_;
    }

    template <typename AppT>
    AppT* getOwningAppAs() const
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        return dynamic_cast<AppT*>(owning_app_);
    }

    PipelineChain& getStageChain(const PipelineStage* stage)
    {
        return stage_chains_[stage];
    }

    void setStageChain(const PipelineStage* stage, const PipelineChain& chain)
    {
        stage_chains_[stage] = chain;
    }

    void runStageChain(const PipelineStage* stage)
    {
        auto it = stage_chains_.find(stage);
        if (it != stage_chains_.end())
        {
            it->second(*this);
        }
    }

    const void* getDataPtr() const
    {
        return bytes_.data();
    }

    size_t getNumBytes() const
    {
        return bytes_.size();
    }

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    void setNext(PipelineChainLink* next)
    {
        next_ = next;
    }

    PipelineChainLink* getNext() const
    {
        return next_;
    }

    uint64_t getTick() const
    {
        return tick_;
    }

    SqlBlob getBlob() const
    {
        SqlBlob blob;
        blob.data_ptr = getDataPtr();
        blob.num_bytes = getNumBytes();
        return blob;
    }

    bool compressed() const
    {
        return compressed_;
    }

    void compress(CompressionLevel level = CompressionLevel::DEFAULT)
    {
        if (compressed())
        {
            return;
        }

        auto data_ptr = getDataPtr();
        auto num_bytes = getNumBytes();
        if (!data_ptr || num_bytes == 0)
        {
            bytes_.clear();
            return;
        }

        compressData(data_ptr, num_bytes, bytes_, level);
        compressed_ = true;
    }

    int getCommittedDbId() const
    {
        return committed_db_id_;
    }

    void setCommittedDbId(int db_id)
    {
        if (committed_db_id_ != 0)
        {
            throw DBException("Committed database ID is already set");
        }
        committed_db_id_ = db_id;
    }

    void retire(simdb::ConcurrentQueue<std::vector<char>>& reusable_buffers)
    {
        reusable_buffers.push(std::move(bytes_));
    }

private:
    uint64_t tick_;
    DatabaseManager* db_mgr_ = nullptr;
    PipelineChainLink* next_ = nullptr;
    App* owning_app_ = nullptr;
    int committed_db_id_ = 0;
    std::vector<char> bytes_;
    bool compressed_ = false;
    std::unordered_map<const PipelineStage*, PipelineChain> stage_chains_;
};

} // namespace simdb
