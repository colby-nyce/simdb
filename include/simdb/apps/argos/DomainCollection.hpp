// <DomainCollection.hpp> -*- C++ -*-

#pragma once

#include <memory>

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/apps/argos/Collectables.hpp"

namespace simdb::collection {

/// \class DomainCollection
/// \brief Non-template base that \ref CollectableBase points at; \ref TimeDomainCollection is the
/// concrete per-clock implementation with timestamp + stager.
class DomainCollection
{
public:
    virtual ~DomainCollection() = default;

    /// \brief Let the \ref Collection object give us our collectables.
    /// Keep a separate structure to hold collectables that are to
    /// be auto-collected.
    void addCollectable(
        const std::string& path,
        std::shared_ptr<CollectableBase> collectable,
        bool auto_collect)
    {
        assert(collectable != nullptr);
        assert(std::find(all_collectables_.begin(), all_collectables_.end(), collectable) == all_collectables_.end());
        all_collectables_.push_back(collectable);
        collectables_by_path_[path] = collectable;

        if (auto_collect)
        {
            all_auto_collectables_.insert(collectable.get());
        }
    }

    /// \brief Access the collectable at the given path
    const CollectableBase* getCollectable(const std::string& path, bool must_exist = true)
    {
        auto it = collectables_by_path_.find(path);
        if (it == collectables_by_path_.end())
        {
            if (must_exist)
            {
                throw DBException("Collectable does not exist at path: ") << path;
            }
            return nullptr;
        }
        return it->second.get();
    }

    /// \brief Get all collectable paths
    std::vector<std::string> getCollectablePaths() const
    {
        std::vector<std::string> paths;
        for (const auto& [path, _] : collectables_by_path_)
        {
            paths.push_back(path);
        }
        return paths;
    }

    /// \brief Enable collection for the given collectable
    void enableCollection(CollectableBase* collectable)
    {
        if (isAutoCollectable_(collectable))
        {
            auto it = std::find(enabled_auto_collectables_.begin(),
                                enabled_auto_collectables_.end(),
                                collectable);
            if (it == enabled_auto_collectables_.end())
            {
                enabled_auto_collectables_.push_back(collectable);
            }
        }
        collectable->enabled_ = true;
    }

    /// \brief Disable collection for the given collectable
    void disableCollection(CollectableBase* collectable)
    {
        if (isAutoCollectable_(collectable))
        {
            auto it = std::find(enabled_auto_collectables_.begin(),
                                enabled_auto_collectables_.end(),
                                collectable);
            if (it != enabled_auto_collectables_.end())
            {
                enabled_auto_collectables_.erase(it);
            }
        }
        collectable->enabled_ = false;
    }

    /// \brief Connect the collectables to the CollectorPipeline's main input queue
    virtual void connectToPipeline(ConcurrentQueue<Payload>* pipeline_head) = 0;

    /// \brief Flush all staged data to the pipeline on preTeardown()
    virtual void flushToPipeline() = 0;

    /// \brief Collect everything and send it down the pipeline
    void performCollection()
    {
        for (auto collectable : all_auto_collectables_)
        {
            collectable->autoCollect();
        }
    }

protected:
    DomainCollection() = default;

    const auto& getCollectables_() const
    {
        return all_collectables_;
    }

private:
    bool isAutoCollectable_(CollectableBase* collectable) const
    {
        return all_auto_collectables_.count(collectable) > 0;
    }

    std::vector<std::shared_ptr<CollectableBase>> all_collectables_;
    std::unordered_set<CollectableBase*> all_auto_collectables_;
    std::vector<CollectableBase*> enabled_auto_collectables_;
    std::map<std::string, std::shared_ptr<CollectableBase>> collectables_by_path_;
};

/// \class TimeDomainCollection
/// \brief One clock domain: collectables plus shared \ref Timestamp and pipeline stager.
template <typename TimeT> class TimeDomainCollection : public DomainCollection
{
public:
    explicit TimeDomainCollection(std::shared_ptr<Timestamp<TimeT>> timestamp) :
        timestamp_(std::move(timestamp))
    {}

    void connectToPipeline(ConcurrentQueue<Payload>* pipeline_head) override final
    {
        stager_ = std::make_unique<PipelineStager<TimeT>>(timestamp_.get(), pipeline_head);
        for (auto& collectable : getCollectables_())
        {
            collectable->connectToPipeline(stager_.get());
        }
    }

    void flushToPipeline() override final
    {
        stager_->flush();
    }

private:
    std::shared_ptr<Timestamp<TimeT>> timestamp_;
    std::unique_ptr<PipelineStager<TimeT>> stager_;
};

inline void CollectableBase::enable()
{
    collection_->enableCollection(this);
}

inline void CollectableBase::disable()
{
    collection_->disableCollection(this);
}

} // namespace simdb::collection
