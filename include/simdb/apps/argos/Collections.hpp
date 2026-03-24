// <Collections.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Collection.hpp"
#include "simdb/apps/argos/CollectionPipeline.hpp"
#include "simdb/apps/argos/ElementTreeNode.hpp"
#include "simdb/utils/ValidValue.hpp"
#include "simdb/Exceptions.hpp"

#include <map>
#include <memory>
#include <string>

namespace simdb::collection {

constexpr inline size_t DEFAULT_HEARTBEAT = 10;

/// \class Collections
/// \brief This class holds separate Collection's for each clock
/// domain across all collectables.
class Collections : public CollectionPipelineMeta
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
    Collections(size_t heartbeat = DEFAULT_HEARTBEAT,
                const std::vector<std::string>& enabled_paths = {})
        : heartbeat_(heartbeat)
        , default_enabled_paths_(enabled_paths)
    {
        if (heartbeat_ == 0)
        {
            throw DBException("Cannot use value 0 for collection heartbeat");
        }
    }

    /// \brief Add a collection for one clock domain
    /// \param clk_name Clock name
    /// \param clk_period Clock period
    /// \throw Throws if this clock already had a collection, but with a different
    /// clock period
    template <typename TimeT>
    void addCollection(const std::string& clk_name, size_t clk_period)
    {
        if (clk_periods_.count(clk_name) && clk_periods_[clk_name] != clk_period)
        {
            throw DBException("Cannot add collection for clock '")
                << clk_name << "' with period " << clk_period << ". This clock "
                << "already has a collection with period " << clk_periods_[clk_name];
        }

        auto& collection = clk_collections_[clk_name];
        if (!collection)
        {
            collection = std::make_unique<TimestampedCollection<TimeT>>();
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
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<AutoScalarCollector<CollectableT>>(collection, heartbeat_, scalar);
        collection->addCollectable(path, collectable, true /*auto collect*/);
        updateDataTypeTree_<CollectableT>();
        return collectable;
    }

    /// \brief Same as autoCollectScalar, but only supports manual collection.
    template <typename CollectableT>
    std::shared_ptr<ScalarCollector<CollectableT>> collectScalarManually(
        const std::string& path,
        const std::string& clk_name)
    {
        verifyNoDupPaths_(path);
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<ScalarCollector<CollectableT>>(collection, heartbeat_);
        collection->addCollectable(path, collectable, false /*manually collect*/);
        updateDataTypeTree_<CollectableT>();
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
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<AutoContainerCollector<ContainerT, Sparse>>(collection, heartbeat_, container, expected_capacity);
        collection->addCollectable(path, collectable, true /*auto collect*/);
        updateDataTypeTree_<typename ContainerT::value_type>();
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
        auto collection = getCollection_(clk_name, true /*must exist*/);
        auto collectable = std::make_shared<ContainerCollector<ContainerT, Sparse>>(collection, heartbeat_, expected_capacity);
        collection->addCollectable(path, collectable, false /*manually collect*/);
        updateDataTypeTree_<typename ContainerT::value_type>();
        return collectable;
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

    /// \brief Return the collection for the given clock
    /// \throw Throws if not found and must_exist=true
    Collection* getCollection_(const std::string& clk_name, bool must_exist = false) const
    {
        auto it = clk_collections_.find(clk_name);
        if (it == clk_collections_.end())
        {
            if (must_exist)
            {
                throw DBException("Collection does not exist for clock: ") << clk_name;
            }
            return nullptr;
        }

        return it->second.get();
    }

    /// \brief Walk the given data type recursively and update the data
    /// type tree. This tree looks something like this:
    ///
    ///   root
    ///     builtins
    ///       i
    ///       q
    ///     enums
    ///       Status
    ///         FAILED
    ///         PASSED
    ///       Colors
    ///         RED
    ///         GREEN
    ///         BLUE
    ///     structs
    ///       MyFoo
    ///         q
    ///         Status
    ///       MyBar
    ///         i
    ///         Colors
    ///         MyFoo
    template <typename ValueType>
    void updateDataTypeTree_()
    {
        if constexpr (std::is_trivial_v<ValueType> && std::is_standard_layout_v<ValueType> && !std::is_enum_v<ValueType>)
        {
            // TODO cnyce:
            // 1. Add a new TreeNode subclass called "Enum"            
        }
        // TODO cnyce:
        // 1. Add a new TreeNode subclass called ""
    }

    /// \brief Called when handling the app's postInit()
    void onCollectionStarting(DatabaseManager* db_mgr) override
    {
        // Create TinyStrings and give it to the collections
        tiny_strings_ = std::make_unique<TinyStrings<>>(db_mgr);
        for (const auto & [clk_name, collection] : clk_collections_)
        {
            collection->setTinyStrings(tiny_strings_.get());
        }

        // Write heartbeat
        db_mgr->INSERT(SQL_TABLE("CollectionGlobals"), SQL_VALUES(heartbeat_));

        // Write all elements
        SerializedTree element_tree(std::in_place_type<ElementTreeNode>, "root");
        for (const auto& path : all_collectable_paths_)
        {
            element_tree.createNodes<ElementTreeNode>(path);
        }

        // Before serializing, add under the root node:
        //   builtins
        //   enums
        //   structs
        element_tree.createNode<>("builtins");
        element_tree.createNode<>("enums");
        element_tree.createNode<>("structs");
        element_tree.serialize(db_mgr);
    }

    /// \brief Called when handling the app's preTeardown()
    void onCollectionStopping(DatabaseManager* db_mgr) override
    {
        // TODO cnyce
        (void)db_mgr;
    }

    /// \brief Called when handling the app's postTeardown()
    void onCollectionStopped(DatabaseManager* db_mgr) override
    {
        // TODO cnyce
        (void)db_mgr;
    }

    const size_t heartbeat_;
    const std::vector<std::string> default_enabled_paths_;
    std::map<std::string, std::unique_ptr<Collection>> clk_collections_;
    std::map<std::string, size_t> clk_periods_;
    std::unordered_set<std::string> all_collectable_paths_;
    std::unique_ptr<TinyStrings<>> tiny_strings_;
};

} // namespace simdb::collection
