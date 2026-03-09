#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/apps/argos/CollectionPoints.hpp"
#include "simdb/apps/argos/TreeNode.hpp"
#include "simdb/schema/SchemaDef.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PipelineManager.hpp"
#include "simdb/utils/Compress.hpp"

#include <boost/algorithm/string/split.hpp>

namespace simdb {

/*!
 * \class CollectionMgr
 *
 * \brief This class provides an easy way to handle simulation-wide data collection.
 * All databases produced by this app are readable by the Argos pipeline viewer front-end.
 */
class CollectionMgr : public App
{
public:
    static constexpr auto NAME = "collection-mgr";

    //! \brief Construct with the associated DatabaseManager and heartbeat. The heartbeat
    //! value dictates the max number of cycles that we employ the optimization "only write
    //! to the database if the collected data is different from the last collected data".
    //! This prevents Argos from having to go back more than N cycles to find the last
    //! known value.
    CollectionMgr(DatabaseManager* db_mgr, size_t heartbeat = 100) :
        db_mgr_(db_mgr),
        heartbeat_(heartbeat),
        tiny_strings_(db_mgr)
    {
    }

    //! \brief Populate the schema with the appropriate tables for all the collections.
    static void defineSchema(Schema& schema)
    {
        using dt = SqlDataType;

        auto& globals_tbl = schema.addTable("CollectionGlobals");
        globals_tbl.addColumn("Heartbeat", dt::int32_t);
        globals_tbl.setColumnDefaultValue("Heartbeat", 10);

        auto& clks_tbl = schema.addTable("Clocks");
        clks_tbl.addColumn("Name", dt::string_t);
        clks_tbl.addColumn("Period", dt::int32_t);

        auto& elem_tns_tbl = schema.addTable("ElementTreeNodes");
        elem_tns_tbl.addColumn("Name", dt::string_t);
        elem_tns_tbl.addColumn("ParentID", dt::int32_t);

        auto& collectable_tns_tbl = schema.addTable("CollectableTreeNodes");
        collectable_tns_tbl.addColumn("ElementTreeNodeID", dt::int32_t);
        collectable_tns_tbl.addColumn("ClockID", dt::int32_t);
        collectable_tns_tbl.addColumn("DataType", dt::string_t);
        collectable_tns_tbl.addColumn("Location", dt::string_t);
        collectable_tns_tbl.addColumn("AutoCollected", dt::int32_t);
        collectable_tns_tbl.setColumnDefaultValue("AutoCollected", 0);

        auto& struct_fields_tbl = schema.addTable("StructFields");
        struct_fields_tbl.addColumn("StructName", dt::string_t);
        struct_fields_tbl.addColumn("FieldName", dt::string_t);
        struct_fields_tbl.addColumn("FieldType", dt::string_t);
        struct_fields_tbl.addColumn("FormatCode", dt::int32_t);
        struct_fields_tbl.addColumn("IsAutoColorizeKey", dt::int32_t);
        struct_fields_tbl.addColumn("IsDisplayedByDefault", dt::int32_t);
        struct_fields_tbl.setColumnDefaultValue("IsAutoColorizeKey", 0);
        struct_fields_tbl.setColumnDefaultValue("IsDisplayedByDefault", 1);

        auto& enum_defns_tbl = schema.addTable("EnumDefns");
        enum_defns_tbl.addColumn("EnumName", dt::string_t);
        enum_defns_tbl.addColumn("EnumValStr", dt::string_t);
        enum_defns_tbl.addColumn("EnumValBlob", dt::blob_t);
        enum_defns_tbl.addColumn("IntType", dt::string_t);

        auto& string_map_tbl = schema.addTable("StringMap");
        string_map_tbl.addColumn("IntVal", dt::int32_t);
        string_map_tbl.addColumn("String", dt::string_t);

        auto& collection_records_tbl = schema.addTable("CollectionRecords");
        collection_records_tbl.addColumn("Tick", dt::uint64_t);
        collection_records_tbl.addColumn("Data", dt::blob_t);
        collection_records_tbl.addColumn("OldestReferredTick", dt::uint64_t);
        collection_records_tbl.setColumnDefaultValue("OldestReferredTick", 0);
        collection_records_tbl.createIndexOn("Tick");

        auto& queue_max_sizes_tbl = schema.addTable("QueueMaxSizes");
        queue_max_sizes_tbl.addColumn("CollectableTreeNodeID", dt::int32_t);
        queue_max_sizes_tbl.addColumn("MaxSize", dt::int32_t);
    }

    //! \brief Add a new clock domain for collection.
    void addClock(const std::string& name, const uint32_t period)
    {
        auto [it, inserted] = clocks_.insert({name, period});
        if (!inserted && it->second != period)
        {
            throw DBException("Clock '") << name << "' already registered with period "
                << it->second << ". Cannot change period to " << period << ".";
        }
    }

    //! \brief Create a collection point for a POD or struct-like type.
    template <typename T>
    std::shared_ptr<CollectionPoint> createCollectable(const std::string& path, const std::string& clock)
    {
        auto treenode = updateTree_(path, clock);
        auto elem_id = treenode->db_id;
        auto clk_id = treenode->clk_id;

        using value_type = type_traits::remove_any_pointer_t<T>;

        std::string dtype;
        if constexpr (std::is_same_v<value_type, bool>)
        {
            dtype = "bool";
        } else if constexpr (std::is_trivial_v<value_type>)
        {
            dtype = getFieldDTypeStr(getFieldDTypeEnum<value_type>());
        } else
        {
            dtype = demangle(typeid(value_type).name());
        }

        auto collectable = std::make_shared<CollectionPoint>(elem_id, clk_id, heartbeat_, dtype, &tiny_strings_);

        if constexpr (!std::is_trivial<value_type>::value)
        {
            StructSerializer<value_type>::getInstance()->serializeDefn(db_mgr_);
        }

        collectables_.push_back(collectable);
        collectables_by_path_[path] = collectable.get();

        return collectable;
    }

    // Automatically collect iterable data (non-POD types).
    template <typename T, bool Sparse>
    std::shared_ptr<std::conditional_t<Sparse, SparseIterableCollectionPoint, ContigIterableCollectionPoint>>
    createIterableCollector(const std::string& path, const std::string& clock, const size_t capacity)
    {
        auto treenode = updateTree_(path, clock);
        auto elem_id = treenode->db_id;
        auto clk_id = treenode->clk_id;

        using value_type = type_traits::remove_any_pointer_t<typename T::value_type>;

        if constexpr (!std::is_trivial<value_type>::value)
        {
            StructSerializer<value_type>::getInstance()->serializeDefn(db_mgr_);
        }

        std::string dtype;
        if constexpr (std::is_same_v<value_type, bool>)
        {
            dtype = "bool";
        } else if constexpr (std::is_trivial_v<value_type>)
        {
            dtype = getFieldDTypeStr(getFieldDTypeEnum<value_type>());
        } else
        {
            dtype = demangle(typeid(value_type).name());
        }

        if constexpr (Sparse)
        {
            dtype += "_sparse";
        } else
        {
            dtype += "_contig";
        }

        dtype += "_capacity" + std::to_string(capacity);

        using collection_point_type =
            std::conditional_t<Sparse, SparseIterableCollectionPoint, ContigIterableCollectionPoint>;
        auto collectable = std::make_shared<collection_point_type>(elem_id, clk_id, heartbeat_, dtype, capacity, &tiny_strings_);
        collectables_.push_back(collectable);
        collectables_by_path_[path] = collectable.get();
        return collectable;
    }

    // Create the pipeline which processes all collected data.
    void createPipeline(pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);

        pipeline->addStage<CompressionStage>("compressor");
        pipeline->addStage<DatabaseStage>("db_writer");
        pipeline->noMoreStages();

        pipeline->bind("compressor.compressed_data", "db_writer.data_to_write");
        pipeline->noMoreBindings();

        // As soon as we call noMoreBindings(), all input/output queues are available.
        pipeline_head_ = pipeline->getInPortQueue<DatabaseEntry>("compressor.input_data");
    }

    // Sweep the collection system for all active collectables that exist on
    // the given clock, and send their data to the database.
    void sweep(const std::string& clk, uint64_t tick)
    {
        const auto clk_id = clock_db_ids_by_name_.at(clk);
        uint64_t oldest_referred_tick = std::numeric_limits<uint64_t>::max();

        std::vector<char> swept_data;
        for (auto& collectable : collectables_)
        {
            if (collectable->getClockId() == clk_id)
            {
                collectable->sweep(swept_data);

                auto oldest_tick = collectable->getLastCollectedTick();
                if (oldest_tick > 0)
                {
                    oldest_referred_tick = std::min(oldest_referred_tick, oldest_tick);
                }
            }
        }

        if (swept_data.empty())
        {
            return;
        }

        DatabaseEntry entry;
        entry.bytes = std::move(swept_data);
        entry.tick = tick;

        if (oldest_referred_tick != std::numeric_limits<uint64_t>::max())
        {
            entry.oldest_referred_tick = oldest_referred_tick;
        }

        pipeline_head_->emplace(std::move(entry));
    }

    // One-time call to write post-simulation metadata to SimDB.
    void postTeardown() override
    {
        for (auto& collectable : collectables_)
        {
            if (auto iterable_collector = dynamic_cast<IterableCollectorBase*>(collectable.get()))
            {
                auto elem_id = iterable_collector->getElemId();
                auto queue_max_size = iterable_collector->getQueueMaxSize();
                db_mgr_->INSERT(SQL_TABLE("QueueMaxSizes"),
                                SQL_VALUES((int)elem_id, (int)queue_max_size));
            }
        }

        tiny_strings_.serialize();
    }

private:
    /// \class DatabaseEntry
    /// \brief Packet type sent down collection pipeline
    struct DatabaseEntry
    {
        std::vector<char> bytes;
        uint64_t tick = std::numeric_limits<uint64_t>::max();
        uint64_t oldest_referred_tick = std::numeric_limits<uint64_t>::max();
    };

    /// Compress on pipeline thread 1
    class CompressionStage : public pipeline::Stage
    {
    public:
        CompressionStage()
        {
            addInPort_<DatabaseEntry>("input_data", input_queue_);
            addOutPort_<DatabaseEntry>("compressed_data", output_queue_);
        }

    private:
        pipeline::PipelineAction run_(bool) override
        {
            DatabaseEntry entry;
            if (input_queue_->try_pop(entry))
            {
                DatabaseEntry compressed_entry;
                compressed_entry.tick = entry.tick;
                compressed_entry.oldest_referred_tick = entry.oldest_referred_tick;
                compressData(entry.bytes, compressed_entry.bytes);

                output_queue_->emplace(std::move(compressed_entry));
                return simdb::pipeline::PROCEED;
            }

            return simdb::pipeline::SLEEP;
        }

        ConcurrentQueue<DatabaseEntry>* input_queue_ = nullptr;
        ConcurrentQueue<DatabaseEntry>* output_queue_ = nullptr;
    };

    /// Write to SQLite on dedicated database thread.
    class DatabaseStage : public pipeline::DatabaseStage<CollectionMgr>
    {
    public:
        DatabaseStage()
        {
            addInPort_<DatabaseEntry>("data_to_write", input_queue_);
        }

    private:
        pipeline::PipelineAction run_(bool) override
        {
            DatabaseEntry entry;
            if (input_queue_->try_pop(entry))
            {
                auto inserter = getTableInserter_("CollectionRecords");
                inserter->createRecordWithColValues(
                    entry.tick,
                    entry.bytes,
                    entry.oldest_referred_tick);

                return pipeline::PROCEED;
            }
            return pipeline::SLEEP;
        }

        ConcurrentQueue<DatabaseEntry>* input_queue_ = nullptr;
    };

    /// Update the collection tree piecemeal as the manager creates new collection points.
    TreeNode* updateTree_(const std::string& path, const std::string& clk)
    {
        if (!root_)
        {
            root_ = std::make_unique<TreeNode>("root");
            auto record =
                db_mgr_->INSERT(SQL_TABLE("ElementTreeNodes"), SQL_COLUMNS("Name", "ParentID"), SQL_VALUES("root", 0));
            root_->db_id = record->getId();
        }

        if (clock_db_ids_by_name_.find(clk) == clock_db_ids_by_name_.end())
        {
            auto period = clocks_.at(clk);
            auto record = db_mgr_->INSERT(SQL_TABLE("Clocks"), SQL_COLUMNS("Name", "Period"), SQL_VALUES(clk, period));
            clock_db_ids_by_name_[clk] = record->getId();
        }

        auto node = root_.get();
        std::vector<std::string> path_parts;
        boost::split(path_parts, path, boost::is_any_of("."));
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

                auto record = db_mgr_->INSERT(SQL_TABLE("ElementTreeNodes"), SQL_COLUMNS("Name", "ParentID"),
                                            SQL_VALUES(part, node->parent->db_id));

                node->db_id = record->getId();
                if (part_idx == path_parts.size() - 1)
                {
                    node->clk_id = clock_db_ids_by_name_.at(clk);
                }
            }
        }

        return node;
    }

    /// The DatabaseManager that we are collecting data for.
    DatabaseManager* db_mgr_;

    /// The max number of cycles that we employ the optimization "only write to the
    /// database if the collected data is different from the last collected data".
    /// This prevents Argos from having to go back more than N cycles to find the
    /// last known value.
    const size_t heartbeat_;

    /// All registered clocks (name->period).
    std::unordered_map<std::string, uint32_t> clocks_;

    /// The root of the serialized element tree.
    std::unique_ptr<TreeNode> root_;

    /// Mapping of clock names to clock IDs.
    std::unordered_map<std::string, int> clock_db_ids_by_name_;

    /// All collectables.
    std::vector<std::shared_ptr<CollectionPointBase>> collectables_;

    /// Mapping of collectable paths to the collectable objects.
    std::unordered_map<std::string, CollectionPointBase*> collectables_by_path_;

    /// Mapping from strings to integer ID.
    TinyStrings<> tiny_strings_;

    /// Main entry point to the pipeline.
    ConcurrentQueue<DatabaseEntry>* pipeline_head_ = nullptr;
};

} // namespace simdb
