// <Collection.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Timestamps.hpp"
#include "simdb/apps/argos/Collectables.hpp"

namespace simdb::collection {

/// \class Collection
/// \brief Base class for all timestamped collections. Subclasses
/// will be templated on the specific <TimeT> type.
class Collection
{
public:
    virtual ~Collection() = default;

    /// \brief Let the Collections object give us our collectables.
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
    /// Not meant to be directly instantiated
    Collection() = default;

    /// Get all collectables
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

/// \class TimestampedCollection
/// \brief Collection with time values of a specific type e.g. double, uint64_t, ...
template <typename TimeT> class TimestampedCollection : public Collection
{
public:
    /// \brief Use a backpointer to get the current time
    void timestampWith(const TimeT* backpointer) { timestamp_ = std::make_unique<Timestamp<TimeT>>(backpointer); }

    /// \brief Use a C-style function pointer to get the current time
    void timestampWith(TimeT (*fn)()) { timestamp_ = std::make_unique<Timestamp<TimeT>>(fn); }

    /// \brief Use a C-style function pointer to get the current time
    void timestampWith(std::function<TimeT()> fn) { timestamp_ = std::make_unique<Timestamp<TimeT>>(fn); }

    /// \brief Connect the collectables to the CollectorPipeline's main input queue
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
    std::unique_ptr<Timestamp<TimeT>> timestamp_;
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
