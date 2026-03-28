// <Collection.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/DomainCollection.hpp"
#include "simdb/apps/argos/CollectionPipeline.hpp"
#include "simdb/apps/argos/DataTypeInspector.hpp"
#include "simdb/apps/argos/DataTypeSerializer.hpp"
#include "simdb/utils/Tree.hpp"
#include "simdb/Exceptions.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace simdb::collection {

namespace detail {

/// Strip one level of smart pointer (and raw pointer) so container \c value_types like
/// \c std::shared_ptr<T> register the same data-type schema as \c T.
template <typename T>
struct dtype_register_element {
    using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

template <typename T>
struct dtype_register_element<std::shared_ptr<T>> : dtype_register_element<T>
{};

template <typename T>
struct dtype_register_element<std::unique_ptr<T>> : dtype_register_element<T>
{};

template <typename T>
struct dtype_register_element<T*> : dtype_register_element<T>
{};

} // namespace detail

constexpr inline size_t DEFAULT_HEARTBEAT = 10;

/// \class Collection
/// \brief Holds one \ref TimeT for all clock domains and a \ref TimestampedCollection per domain.
template <typename TimeT> class Collection : public CollectionPipelineHelper
{
public:
    /// \brief Construct
    /// \param heartbeat Size/speed tunable parameter; gives the max number
    /// of times a collectable can use its disk space optimization strategy,
    /// at which point its data must be forcible captured. Higher heartbeats
    /// lead to smaller databases but the Argos UI will be less responsive.
    /// \param enabled_paths If given, only the collectables at these paths
    /// will be collected by default. Other collectables that were created
    /// outside this list will have to be explicitly enabled during simulation
    /// via the CollectableBase::enable() method.
    Collection(size_t heartbeat = DEFAULT_HEARTBEAT,
               const std::vector<std::string>& enabled_paths = {})
        : heartbeat_(heartbeat)
        , default_enabled_paths_(enabled_paths)
    {
        if (heartbeat_ == 0)
        {
            throw DBException("Cannot use value 0 for collection heartbeat");
        }
    }

    /// \brief Use a backpointer to get the current time for every clock domain.
    /// \throw DBException if any clock domain has already been added via addCollection().
    void timestampWith(const TimeT* backpointer)
    {
        ensureTimestampReconfigurable_();
        timestamp_ = std::make_shared<Timestamp<TimeT>>(backpointer);
    }

    /// \brief Use a C-style function pointer to get the current time for every clock domain.
    /// \throw DBException if any clock domain has already been added via addCollection().
    void timestampWith(TimeT (*fn)())
    {
        ensureTimestampReconfigurable_();
        timestamp_ = std::make_shared<Timestamp<TimeT>>(fn);
    }

    /// \brief Use a \c std::function to get the current time for every clock domain.
    /// \throw DBException if any clock domain has already been added via addCollection().
    void timestampWith(std::function<TimeT()> fn)
    {
        ensureTimestampReconfigurable_();
        timestamp_ = std::make_shared<Timestamp<TimeT>>(std::move(fn));
    }

    /// \brief Add a collection for one clock domain
    /// \param clk_name Clock name
    /// \param clk_period Clock period
    /// \throw Throws if this clock already had a collection, but with a different
    /// clock period
    void addCollection(const std::string& clk_name, size_t clk_period)
    {
        if (clk_periods_.count(clk_name) && clk_periods_[clk_name] != clk_period)
        {
            throw DBException("Cannot add collection for clock '")
                << clk_name << "' with period " << clk_period << ". This clock "
                << "already has a collection with period " << clk_periods_[clk_name];
        }

        if (!timestamp_)
        {
            throw DBException("Must call timestampWith() before addCollection()");
        }

        auto& collection = collections_[clk_name];
        if (!collection)
        {
            collection = std::make_unique<TimestampedCollection<TimeT>>(timestamp_);
            clk_periods_[clk_name] = clk_period;
        }
    }

    /// \brief Create a collectable for a scalar (integral/floating-point/enum/string/bool, or a
    /// struct-like object containing these types). Supports auto-collection and manual collection.
    /// \param path Dot-delimited path to the scalar that uniquely defines where this variable
    /// lives in the simulator.
    /// \param clk_name Name of the clock this collection point belongs to. Must have already
    /// called addCollection() with this clock name.
    /// \throw Throws if collection does not exist for the given clock.
    template <typename CollectableT>
    std::shared_ptr<AutoScalarCollector<CollectableT>> collectScalarWithAutoCollection(
        const std::string& path,
        const std::string& clk_name,
        const CollectableT* scalar)
    {
        verifyNoDupPaths_(path);
        auto dtype_hier = dtype_inspector_.registerType<CollectableT>();
        collectables_tree_.createNodes(path);
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<AutoScalarCollector<CollectableT>>(
            collection, heartbeat_, std::move(dtype_hier), scalar);
        collection->addCollectable(path, collectable, true /*auto collect*/);
        return collectable;
    }

    /// \brief Same as autoCollectScalar, but only supports manual collection.
    template <typename CollectableT>
    std::shared_ptr<ScalarCollector<CollectableT>> collectScalarManually(
        const std::string& path,
        const std::string& clk_name)
    {
        verifyNoDupPaths_(path);
        auto dtype_hier = dtype_inspector_.registerType<CollectableT>();
        collectables_tree_.createNodes(path);
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable =
            std::make_shared<ScalarCollector<CollectableT>>(collection, heartbeat_, std::move(dtype_hier));
        collection->addCollectable(path, collectable, false /*manually collect*/);
        return collectable;
    }

    /// \brief Create a collectable for a queue-like data structure, where ValueType is
    /// either integral, floating-point, enum, string, bool, or a struct-like object
    /// containing these types.
    template <typename ContainerT, bool Sparse>
    std::shared_ptr<AutoContainerCollector<ContainerT, Sparse>> collectContainerWithAutoCollection(
        const std::string& path,
        const std::string& clk_name,
        const ContainerT* container,
        size_t expected_capacity)
    {
        verifyNoDupPaths_(path);
        using ElemT = typename detail::dtype_register_element<typename ContainerT::value_type>::type;
        auto dtype_hier = dtype_inspector_.registerType<ElemT>();
        collectables_tree_.createNodes(path);
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<AutoContainerCollector<ContainerT, Sparse>>(
            collection, heartbeat_, container, expected_capacity, std::move(dtype_hier));
        collection->addCollectable(path, collectable, true /*auto collect*/);
        return collectable;
    }

    /// \brief Same as autoCollectQueue, but only supports manual collection.
    template <typename ContainerT, bool Sparse>
    std::shared_ptr<ContainerCollector<ContainerT, Sparse>> collectContainerManually(
        const std::string& path,
        const std::string& clk_name,
        size_t expected_capacity)
    {
        verifyNoDupPaths_(path);
        using ElemT = typename detail::dtype_register_element<typename ContainerT::value_type>::type;
        auto dtype_hier = dtype_inspector_.registerType<ElemT>();
        collectables_tree_.createNodes(path);
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<ContainerCollector<ContainerT, Sparse>>(
            collection, heartbeat_, expected_capacity, std::move(dtype_hier));
        collection->addCollectable(path, collectable, false /*manually collect*/);
        return collectable;
    }

    /// \brief Connect the collectables to the CollectorPipeline's main input queue
    void connectToPipeline(ConcurrentQueue<Payload>* pipeline_head)
    {
        for (auto& [_, collection] : collections_)
        {
            collection->connectToPipeline(pipeline_head);
        }
    }

private:
    /// \brief Verify that all collectables are uniquely owned by clock-specific
    /// TimestampedCollection's
    void verifyNoDupPaths_(const std::string& path)
    {
        if (!all_collectable_paths_.insert(path).second)
        {
            throw DBException("Collectable already exists at path: ") << path;
        }
    }

    void ensureTimestampReconfigurable_() const
    {
        if (!collections_.empty())
        {
            throw DBException("timestampWith() cannot be called after addCollection()");
        }
    }

    /// \brief Return the collection for the given clock
    /// \throw Throws if not found and must_exist=true
    DomainCollection* getCollection_(const std::string& clk_name, bool must_exist = false) const
    {
        auto it = collections_.find(clk_name);
        if (it == collections_.end())
        {
            if (must_exist)
            {
                throw DBException("Collection does not exist for clock: ") << clk_name;
            }
            return nullptr;
        }

        return it->second.get();
    }

    /// \brief Called when handling the app's postInit()
    void writeMetaOnPostInit(DatabaseManager* db_mgr) override
    {
        // Write heartbeat
        db_mgr->INSERT(SQL_TABLE("CollectionGlobals"), SQL_VALUES(heartbeat_));

        db_mgr->safeTransaction([&]() {
            std::map<simdb::Tree::TreeNode*, int> element_db_ids;
            collectables_tree_.dfs([&](simdb::Tree::TreeNode* node) {
                auto parent_id = 0;
                if (auto* parent = node->getParent())
                {
                    parent_id = element_db_ids.at(parent);
                }
                auto rec = db_mgr->INSERT(
                    SQL_TABLE("ElementTreeNodes"),
                    SQL_VALUES(parent_id, node->getName()));
                element_db_ids[node] = rec->getId();
                return true;
            });

            std::map<std::string, int> clock_db_ids;
            for (const auto& [clk_name, period] : clk_periods_)
            {
                auto coll_it = collections_.find(clk_name);
                if (coll_it == collections_.end() || !coll_it->second)
                {
                    continue;
                }
                auto clk_rec = db_mgr->INSERT(
                    SQL_TABLE("Clocks"),
                    SQL_VALUES(clk_name, static_cast<int32_t>(period)));
                clock_db_ids[clk_name] = clk_rec->getId();
            }

            for (const auto& [clk_name, collection_ptr] : collections_)
            {
                if (!collection_ptr)
                {
                    continue;
                }
                const int clock_id = clock_db_ids.at(clk_name);
                for (const auto& path : collection_ptr->getCollectablePaths())
                {
                    auto* elem_node = collectables_tree_.tryGet(path, true);
                    const int elem_tree_id = element_db_ids.at(elem_node);
                    const auto* coll = collection_ptr->getCollectable(path);
                    (void)db_mgr->INSERT(
                        SQL_TABLE("CollectableTreeNodes"),
                        SQL_VALUES(
                            elem_tree_id,
                            clock_id,
                            coll->collectableTypeNameForDb(),
                            coll->collectableAutoCollectedForDb()));
                }
            }
        });

        dtype_inspector_.bindDatabase(db_mgr);

        // Write data types and their hierarchies (structs / nested structs)
        DataTypeSerializer::serialize(&dtype_inspector_, db_mgr);
    }

    void openStage(ConcurrentQueue<Payload>* pipeline_head) override
    {
        for (auto& [_, collection] : collections_)
        {
            collection->connectToPipeline(pipeline_head);
        }
    }

    void flushStageToPipeline() override
    {
        for (auto& [_, collection] : collections_)
        {
            collection->flushToPipeline();
        }
    }

    const size_t heartbeat_;
    const std::vector<std::string> default_enabled_paths_;
    std::map<std::string, std::unique_ptr<TimestampedCollection<TimeT>>> collections_;
    std::map<std::string, size_t> clk_periods_;
    std::unordered_set<std::string> all_collectable_paths_;
    DataTypeInspector dtype_inspector_;
    simdb::Tree collectables_tree_;

    std::shared_ptr<Timestamp<TimeT>> timestamp_;
};

} // namespace simdb::collection
