#pragma once

// TODO cnyce: Implement this as an App.

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/serialize/CollectionPoints.hpp"
#include "simdb/serialize/Serialize.hpp"
#include "simdb/serialize/ThreadedSink.hpp"
#include "simdb/serialize/TreeNode.hpp"

namespace simdb
{

/*!
 * \class CollectionMgr
 *
 * \brief This class provides an easy way to handle simulation-wide data collection.
 */
class CollectionMgr
{
public:
    /// Construct with the DatabaseManager and SQLiteTransaction.
    ///
    /// By default, we use 1 compression thread and 1 database thread. SimDB does not
    /// allow the database thread to be disabled, but you can control the max number
    /// of compression threads (including zero).
    ///
    /// Compression levels will be dialed up/down automatically across the threads
    /// in response to fast/slow simulation data rates until it balances out at
    /// optimal performance.
    CollectionMgr(DatabaseManager* db_mgr, size_t heartbeat, size_t num_compression_threads = 1);

    /// Add a new clock domain for collection.
    void addClock(const std::string& name, const uint32_t period);

    /// Populate the schema with the appropriate tables for all the collections.
    void defineSchema(Schema& schema) const;

    /// Create a collection point for a POD or struct-like type.
    template <typename T> std::shared_ptr<CollectionPoint> createCollectable(const std::string& path, const std::string& clock);

    // Automatically collect iterable data (non-POD types).
    template <typename T, bool Sparse>
    std::shared_ptr<std::conditional_t<Sparse, SparseIterableCollectionPoint, ContigIterableCollectionPoint>>
    createIterableCollector(const std::string& path, const std::string& clock, const size_t capacity);

    // Sweep the collection system for all active collectables that exist on
    // the given clock, and send their data to the database.
    void sweep(const std::string& clk,
               uint64_t tick,
               DatabaseEntryCallback post_process_callback = nullptr,
               void* post_process_user_data = nullptr);

    // Add arbitrary code to be invoked on the database thread inside
    // a BEGIN/COMMIT TRANSACTION block.
    void queueWork(const AnyDatabaseWork& work)
    {
        sink_.queueWork(work);
    }

    // One-time call to write post-simulation metadata to SimDB.
    void postSim();

private:
    /// tree piecemeal as the simulator gets access to all the collection
    /// points it needs.
    ///
    /// Returns the ElementTreeNodeID for the given path.
    TreeNode* updateTree_(const std::string& path, const std::string& clk);

    /// One-time call to get the collection system ready.
    void finalizeCollections_();

    /// The DatabaseManager that we are collecting data for.
    DatabaseManager* db_mgr_;

    /// The max number of cycles that we employ the optimization "only write to the
    /// database if the collected data is different from the last collected data".
    /// This prevents Argos from having to go back more than N cycles to find the
    /// last known value.
    const size_t heartbeat_;

    /// All registered clocks (name->period).
    std::unordered_map<std::string, uint32_t> clocks_;

    /// All collectables.
    std::vector<std::shared_ptr<CollectionPointBase>> collectables_;

    /// Mapping of collectable paths to the collectable objects.
    std::unordered_map<std::string, CollectionPointBase*> collectables_by_path_;

    /// All collected data in the call to sweep().
    std::vector<char> swept_data_;

    /// All compressed data in the call to sweep().
    std::vector<char> compressed_swept_data_;

    /// Data sink for high-performance processing (compression + SimDB writes)
    ThreadedSink sink_;

    /// The root of the serialized element tree.
    std::unique_ptr<TreeNode> root_;

    /// Mapping of clock names to clock IDs.
    std::unordered_map<std::string, int> clock_db_ids_by_name_;

    friend class DatabaseManager;
};

/// Note that this method is defined here since we need the INSERT() method.
inline void FieldBase::serializeDefn(DatabaseManager* db_mgr, const std::string& struct_name) const
{
    const auto field_dtype_str = getFieldDTypeStr(dtype_);
    const auto fmt = static_cast<int>(format_);
    const auto is_autocolorize_key = (int)isAutocolorizeKey();
    const auto is_displayed_by_default = (int)isDisplayedByDefault();

    db_mgr->INSERT(SQL_TABLE("StructFields"),
                   SQL_COLUMNS("StructName", "FieldName", "FieldType", "FormatCode", "IsAutoColorizeKey", "IsDisplayedByDefault"),
                   SQL_VALUES(struct_name, name_, field_dtype_str, fmt, is_autocolorize_key, is_displayed_by_default));
}

/// Note that this method is defined here since we need the INSERT() method.
template <typename EnumT> inline void EnumMap<EnumT>::serializeDefn(DatabaseManager* db_mgr) const
{
    using enum_int_t = typename std::underlying_type<EnumT>::type;

    if (!serialized_)
    {
        auto dtype = getFieldDTypeEnum<enum_int_t>();
        auto int_type_str = getFieldDTypeStr(dtype);

        for (const auto& kvp : *map_)
        {
            auto enum_val_str = kvp.first;
            auto enum_val_vec = convertIntToBlob<enum_int_t>(kvp.second);

            SqlBlob enum_val_blob;
            enum_val_blob.data_ptr = enum_val_vec.data();
            enum_val_blob.num_bytes = enum_val_vec.size();

            db_mgr->INSERT(SQL_TABLE("EnumDefns"),
                           SQL_COLUMNS("EnumName", "EnumValStr", "EnumValBlob", "IntType"),
                           SQL_VALUES(enum_name_, enum_val_str, enum_val_blob, int_type_str));
        }

        serialized_ = true;
    }
}

/// Note that this method is defined here since we need the INSERT() method.
template <typename EnumT> inline void EnumField<EnumT>::serializeDefn(DatabaseManager* db_mgr, const std::string& struct_name) const
{
    const auto field_name = getName();
    const auto is_autocolorize_key = (int)isAutocolorizeKey();
    const auto is_displayed_by_default = (int)isDisplayedByDefault();

    db_mgr->INSERT(SQL_TABLE("StructFields"),
                   SQL_COLUMNS("StructName", "FieldName", "FieldType", "IsAutoColorizeKey", "IsDisplayedByDefault"),
                   SQL_VALUES(struct_name, field_name, enum_name_, is_autocolorize_key, is_displayed_by_default));

    EnumMap<EnumT>::instance()->serializeDefn(db_mgr);
}

inline CollectionMgr::CollectionMgr(DatabaseManager* db_mgr, size_t heartbeat, size_t num_compression_threads)
    : db_mgr_(db_mgr)
    , heartbeat_(heartbeat)
    , sink_(db_mgr, num_compression_threads)
{
}

inline void CollectionMgr::addClock(const std::string& name, const uint32_t period)
{
    clocks_[name] = period;
}

inline void CollectionMgr::defineSchema(Schema& schema) const
{
    using dt = SqlDataType;

    schema.addTable("CollectionGlobals").addColumn("Heartbeat", dt::int32_t).setColumnDefaultValue("Heartbeat", 10);

    schema.addTable("Clocks").addColumn("Name", dt::string_t).addColumn("Period", dt::int32_t);

    schema.addTable("ElementTreeNodes").addColumn("Name", dt::string_t).addColumn("ParentID", dt::int32_t);

    schema.addTable("CollectableTreeNodes")
        .addColumn("ElementTreeNodeID", dt::int32_t)
        .addColumn("ClockID", dt::int32_t)
        .addColumn("DataType", dt::string_t)
        .addColumn("Location", dt::string_t)
        .addColumn("AutoCollected", dt::int32_t)
        .setColumnDefaultValue("AutoCollected", 0)
        .disableAutoIncPrimaryKey();

    schema.addTable("StructFields")
        .addColumn("StructName", dt::string_t)
        .addColumn("FieldName", dt::string_t)
        .addColumn("FieldType", dt::string_t)
        .addColumn("FormatCode", dt::int32_t)
        .addColumn("IsAutoColorizeKey", dt::int32_t)
        .addColumn("IsDisplayedByDefault", dt::int32_t)
        .setColumnDefaultValue("IsAutoColorizeKey", 0)
        .setColumnDefaultValue("IsDisplayedByDefault", 1);

    schema.addTable("EnumDefns")
        .addColumn("EnumName", dt::string_t)
        .addColumn("EnumValStr", dt::string_t)
        .addColumn("EnumValBlob", dt::blob_t)
        .addColumn("IntType", dt::string_t);

    schema.addTable("StringMap").addColumn("IntVal", dt::int32_t).addColumn("String", dt::string_t);

    schema.addTable("CollectionRecords")
        .addColumn("Tick", dt::int64_t)
        .addColumn("Data", dt::blob_t)
        .addColumn("IsCompressed", dt::int32_t)
        .createIndexOn("Tick");

    schema.addTable("QueueMaxSizes").addColumn("CollectableTreeNodeID", dt::int32_t).addColumn("MaxSize", dt::int32_t);
}

template <typename T>
inline std::shared_ptr<CollectionPoint> CollectionMgr::createCollectable(const std::string& path, const std::string& clock)
{
    auto treenode = updateTree_(path, clock);
    treenode->is_collectable = true;
    auto elem_id = treenode->db_id;
    auto clk_id = treenode->clk_id;

    using value_type = meta_utils::remove_any_pointer_t<T>;

    std::string dtype;
    if constexpr (std::is_same_v<value_type, bool>)
    {
        dtype = "bool";
    }
    else if constexpr (std::is_trivial_v<value_type>)
    {
        dtype = getFieldDTypeStr(getFieldDTypeEnum<value_type>());
    }
    else
    {
        dtype = demangle(typeid(value_type).name());
    }

    auto collectable = std::make_shared<CollectionPoint>(elem_id, clk_id, heartbeat_, dtype);

    if constexpr (!std::is_trivial<value_type>::value)
    {
        StructSerializer<value_type>::getInstance()->serializeDefn(db_mgr_);
    }

    collectables_.push_back(collectable);
    collectables_by_path_[path] = collectable.get();

    return collectable;
}

template <typename T, bool Sparse>
std::shared_ptr<std::conditional_t<Sparse, SparseIterableCollectionPoint, ContigIterableCollectionPoint>>
CollectionMgr::createIterableCollector(const std::string& path, const std::string& clock, const size_t capacity)
{
    auto treenode = updateTree_(path, clock);
    treenode->is_collectable = true;
    auto elem_id = treenode->db_id;
    auto clk_id = treenode->clk_id;

    using value_type = meta_utils::remove_any_pointer_t<typename T::value_type>;

    if constexpr (!std::is_trivial<value_type>::value)
    {
        StructSerializer<value_type>::getInstance()->serializeDefn(db_mgr_);
    }

    std::string dtype;
    if constexpr (std::is_same_v<value_type, bool>)
    {
        dtype = "bool";
    }
    else if constexpr (std::is_trivial_v<value_type>)
    {
        dtype = getFieldDTypeStr(getFieldDTypeEnum<value_type>());
    }
    else
    {
        dtype = demangle(typeid(value_type).name());
    }

    if constexpr (Sparse)
    {
        dtype += "_sparse";
    }
    else
    {
        dtype += "_contig";
    }

    dtype += "_capacity" + std::to_string(capacity);

    using collection_point_type = std::conditional_t<Sparse, SparseIterableCollectionPoint, ContigIterableCollectionPoint>;
    auto collectable = std::make_shared<collection_point_type>(elem_id, clk_id, heartbeat_, dtype, capacity);
    collectables_.push_back(collectable);
    collectables_by_path_[path] = collectable.get();
    return collectable;
}

/// Sweep the collection system for all active collectables that exist on
/// the given clock, and send their data to the database.
inline void CollectionMgr::sweep(const std::string& clk, uint64_t tick,
                                 DatabaseEntryCallback post_process_callback,
                                 void* post_process_user_data)
{
    const auto clk_id = clock_db_ids_by_name_.at(clk);

    swept_data_.clear();
    for (auto& collectable : collectables_)
    {
        if (collectable->getClockId() == clk_id)
        {
            collectable->sweep(swept_data_);
        }
    }

    if (swept_data_.empty())
    {
        return;
    }

    DatabaseEntry entry;
    entry.bytes = std::move(swept_data_);
    entry.compressed = false;
    entry.tick = tick;
    entry.post_process_callback = post_process_callback;
    entry.post_process_user_data = post_process_user_data;

    sink_.push(std::move(entry));
}

/// One-time call to write post-simulation metadata to SimDB.
inline void CollectionMgr::postSim()
{
    db_mgr_->safeTransaction(
        [&]()
        {
            for (auto& collectable : collectables_)
            {
                collectable->postSim(db_mgr_);
            }
            return true;
        });

    sink_.teardown();
}

/// Note that this method is defined here since we need the INSERT() method.
inline void ContigIterableCollectionPoint::postSim(DatabaseManager* db_mgr)
{
    db_mgr->INSERT(SQL_TABLE("QueueMaxSizes"), SQL_COLUMNS("CollectableTreeNodeID", "MaxSize"), SQL_VALUES(getElemId(), queue_max_size_));
}

/// Note that this method is defined here since we need the INSERT() method.
inline void SparseIterableCollectionPoint::postSim(DatabaseManager* db_mgr)
{
    db_mgr->INSERT(SQL_TABLE("QueueMaxSizes"), SQL_COLUMNS("CollectableTreeNodeID", "MaxSize"), SQL_VALUES(getElemId(), queue_max_size_));
}

/// Note that this method is defined here since we need the INSERT() method.
inline TreeNode* CollectionMgr::updateTree_(const std::string& path, const std::string& clk)
{
    if (!root_)
    {
        root_ = std::make_unique<TreeNode>("root");
        auto record = db_mgr_->INSERT(SQL_TABLE("ElementTreeNodes"), SQL_COLUMNS("Name", "ParentID"), SQL_VALUES("root", 0));
        root_->db_id = record->getId();
    }

    if (clock_db_ids_by_name_.find(clk) == clock_db_ids_by_name_.end())
    {
        auto period = clocks_.at(clk);
        auto record = db_mgr_->INSERT(SQL_TABLE("Clocks"), SQL_COLUMNS("Name", "Period"), SQL_VALUES(clk, period));
        clock_db_ids_by_name_[clk] = record->getId();
    }

    // Function to split_string a string by a delimiter
    auto split_string = [](const std::string& s, char delimiter)
    {
        std::vector<std::string> tokens;
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, delimiter))
        {
            tokens.push_back(item);
        }
        return tokens;
    };

    auto node = root_.get();
    auto path_parts = split_string(path, '.');
    for (size_t part_idx = 0; part_idx < path_parts.size(); ++part_idx)
    {
        auto part = path_parts[part_idx];
        auto found = false;
        for (const auto& child : node->children)
        {
            if (child->name == part)
            {
                node = child.get();
                found = true;
                break;
            }
        }

        if (!found)
        {
            auto new_node = std::make_unique<TreeNode>(part, node);
            node->children.push_back(std::move(new_node));
            node = node->children.back().get();

            auto record =
                db_mgr_->INSERT(SQL_TABLE("ElementTreeNodes"), SQL_COLUMNS("Name", "ParentID"), SQL_VALUES(part, node->parent->db_id));

            node->db_id = record->getId();
            if (part_idx == path_parts.size() - 1)
            {
                node->clk_id = clock_db_ids_by_name_.at(clk);
            }
        }
    }

    return node;
}

/// Note that this method is defined here since we need the INSERT() method.
inline void CollectionMgr::finalizeCollections_()
{
    if (!root_)
    {
        throw DBException("Collection system does not have any collectables!");
    }

    db_mgr_->INSERT(SQL_TABLE("CollectionGlobals"), SQL_COLUMNS("Heartbeat"), SQL_VALUES((int)heartbeat_));

    std::vector<TreeNode*> collectable_nodes;

    std::function<void(TreeNode*)> findCollectableNodes = [&](TreeNode* node)
    {
        if (node->is_collectable)
        {
            collectable_nodes.push_back(node);
        }

        for (auto& child : node->children)
        {
            findCollectableNodes(child.get());
        }
    };

    findCollectableNodes(root_.get());

    for (auto collectable_node : collectable_nodes)
    {
        auto elem_id = collectable_node->db_id;
        auto clk_id = collectable_node->clk_id;
        auto loc = collectable_node->getLocation();
        auto collectable = collectables_by_path_.at(loc);
        auto dtype = collectable->getDataTypeStr();

        db_mgr_->INSERT(SQL_TABLE("CollectableTreeNodes"),
                        SQL_COLUMNS("ElementTreeNodeID", "ClockID", "DataType", "Location"),
                        SQL_VALUES(elem_id, clk_id, dtype, loc));
    }
}

/// Note that this method is defined here since we need the INSERT() method.
inline void DatabaseThread::flush()
{
    db_mgr_->safeTransaction(
        [&]()
        {
            DatabaseEntry entry;
            while (queue_.try_pop(entry))
            {
                const auto& data = entry.bytes;
                const auto tick = entry.tick;
                const auto compressed = entry.compressed;

                const auto record = db_mgr_->INSERT(SQL_TABLE("CollectionRecords"),
                                                    SQL_COLUMNS("Tick", "Data", "IsCompressed"),
                                                    SQL_VALUES(tick, data, (int)compressed));

                if (entry.post_process_callback)
                {
                    entry.post_process_callback(record->getId(), entry.tick, entry.post_process_user_data);
                }

                ++num_processed_;
            }

            AnyDatabaseWork extra_work;
            while (work_queue_.try_pop(extra_work))
            {
                extra_work(db_mgr_);
            }

            for (const auto& kvp : StringMap::instance()->getUnserializedMap())
            {
                db_mgr_->INSERT(SQL_TABLE("StringMap"), SQL_COLUMNS("IntVal", "String"), SQL_VALUES(kvp.first, kvp.second));
            }

            StringMap::instance()->clearUnserializedMap();
            return true;
        });
}

} // namespace simdb

#if 0
    /// Initialize the collection manager prior to calling getCollectionMgr().
    ///
    /// \param heartbeat The maximum number of cycles' worth of repeating (unchanging)
    /// data before we are forced to write the data to the database again. This is used
    /// as a time/space tradeoff, where larger heartbeats yield smaller databases but
    /// a slower Argos UI. The default is 10 cycles, with a valid range of 0 to 25.
    void enableCollection(size_t heartbeat = 10, size_t num_compression_threads = 1)
    {
        if (heartbeat > 25 || heartbeat == 0)
        {
            throw DBException("Invalid heartbeat value. Must be in the range [1, 25]");
        }

        if (!collection_mgr_)
        {
            if (!db_conn_)
            {
                Schema schema;
                createDatabaseFromSchema(schema);
            }

            collection_mgr_ = std::make_unique<CollectionMgr>(this, heartbeat, num_compression_threads);

            Schema schema;
            collection_mgr_->defineSchema(schema);
            appendSchema(schema);
        }
    }

    /// Access the data collection system for e.g. pipeline collection
    /// or stats collection (CSV/JSON).
    CollectionMgr* getCollectionMgr()
    {
        return collection_mgr_.get();
    }

    /// One-time call to get the collection system ready.
    void finalizeCollections()
    {
        safeTransaction([&]() { return finalizeCollections_(); });
    }

    // One-time call to write post-simulation metadata to SimDB.
    void postSim()
    {
        collection_mgr_->postSim();
    }

    /// Finalize the collection system.
    bool finalizeCollections_()
    {
        if (collection_mgr_)
        {
            collection_mgr_->finalizeCollections_();
            return true;
        }
        return false;
    }

    /// Collection manager (CSV/JSON/Argos).
    std::unique_ptr<CollectionMgr> collection_mgr_;

#endif
