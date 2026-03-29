// <CollectionPipeline.hpp> -*- C++ *-*

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/pipeline/PipelineManager.hpp"
#include "simdb/apps/argos/CollectionBase.hpp"
#include "simdb/apps/argos/PipelineStager.hpp"
#include "simdb/utils/TinyStrings.hpp"
#include "simdb/utils/Compress.hpp"

namespace simdb::collection {

/// \class CollectionPipeline
/// \brief SimDB \ref App that registers collection metadata schema and forwards lifecycle events to a \ref CollectionPipelineMeta handler.
///
/// Owns a \ref TinyStrings instance backed by \a db_mgr. The pipeline name is \ref NAME for factory lookup.
class CollectionPipeline : public App
{
public:
    static constexpr auto NAME = "collection-pipeline";

    /// \brief Construct the collection pipeline app.
    /// \param db_mgr Database manager used for schema and persistence.
    /// \param handler Non-owning callbacks; must remain valid for the lifetime of this app (typically supplied via \ref AppFactory::parameterize).
    CollectionPipeline(DatabaseManager* db_mgr, CollectionBase* collection)
        : db_mgr_(db_mgr)
        , tiny_strings_(db_mgr)
        , collection_(collection)
    {}

    /// \class AppFactory
    /// \brief \ref AppFactoryBase that creates \ref CollectionPipeline instances sharing a configured \ref CollectionPipelineMeta.
    class AppFactory : public AppFactoryBase
    {
    public:
        using AppT = CollectionPipeline;

        /// \brief Forward the collection to the pipeline app.
        void parameterize(CollectionBase* collection)
        {
            collection_ = collection;
        }

        /// \brief Allocate a new pipeline app.
        /// \param db_mgr Database manager for the new app.
        /// \return New \ref CollectionPipeline; ownership follows the app framework contract.
        AppT* createApp(DatabaseManager* db_mgr) override
        {
            assert(collection_ != nullptr);
            return new AppT(db_mgr, collection_);
        }

        /// \brief Populate \a schema with the same tables as \ref CollectionPipeline::defineSchema.
        /// \param schema Schema to fill before opening the database.
        void defineSchema(Schema& schema) const override
        {
            AppT::defineSchema(schema);
            auto& timestamps_tbl = schema.addTable("Timestamps");
            timestamps_tbl.addColumn("Timestamp", collection_->getSqlTimeType());
        }

    private:
        CollectionBase* collection_ = nullptr;
    };

    /// \brief Declare SQLite tables used by Argos collection (globals, clocks, element/collectable trees,
    /// data-type metadata, string map, records, queue sizes).
    /// \param schema Schema object to extend.
    static void defineSchema(Schema& schema)
    {
        using dt = SqlDataType;

        auto& globals_tbl = schema.addTable("CollectionGlobals");
        globals_tbl.addColumn("Heartbeat", dt::int32_t);

        auto& dtype_schemas_tbl = schema.addTable("DataTypeSchemas");
        dtype_schemas_tbl.addColumn("RootTypeName", dt::string_t);

        auto& dtype_nodes_tbl = schema.addTable("DataTypeNodes");
        dtype_nodes_tbl.addColumn("SchemaId", dt::int32_t);
        dtype_nodes_tbl.addColumn("ParentId", dt::int32_t);
        dtype_nodes_tbl.addColumn("Kind", dt::string_t);
        dtype_nodes_tbl.addColumn("Name", dt::string_t);
        dtype_nodes_tbl.addColumn("Description", dt::string_t);
        dtype_nodes_tbl.addColumn("TypeName", dt::string_t);
        dtype_nodes_tbl.addColumn("EnumBacking", dt::string_t);

        auto& dtype_enum_members_tbl = schema.addTable("DataTypeEnumMembers");
        dtype_enum_members_tbl.addColumn("EnumNodeId", dt::int32_t);
        dtype_enum_members_tbl.addColumn("MemberName", dt::string_t);
        dtype_enum_members_tbl.addColumn("MemberValue", dt::string_t);

        auto& clks_tbl = schema.addTable("Clocks");
        clks_tbl.addColumn("Name", dt::string_t);
        clks_tbl.addColumn("Period", dt::int32_t);

        auto& elem_tns_tbl = schema.addTable("ElementTreeNodes");
        elem_tns_tbl.addColumn("ParentId", dt::int32_t);
        elem_tns_tbl.addColumn("Name", dt::string_t);

        auto& collectable_tns_tbl = schema.addTable("CollectableTreeNodes");
        collectable_tns_tbl.addColumn("ElementTreeNodeID", dt::int32_t);
        collectable_tns_tbl.addColumn("ClockID", dt::int32_t);
        collectable_tns_tbl.addColumn("TypeName", dt::string_t);
        collectable_tns_tbl.addColumn("AutoCollected", dt::int32_t);

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

    /// \brief Return the string intern table used when mapping string values to stable integer IDs for storage.
    /// \return Pointer to the owned \ref TinyStrings instance (valid for the app lifetime).
    simdb::TinyStrings<>* getTinyStrings()
    {
        return &tiny_strings_;
    }

    /// \brief Create the pipeline to process collected data.
    void createPipeline(pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);

        pipeline->addStage<Organizer>("organizer", collection_->getHeartbeat());
        pipeline->addStage<Compressor>("compressor");
        pipeline->addStage<Writer>("writer");
        pipeline->noMoreStages();

        pipeline->bind("organizer.output_queue", "compressor.input_queue");
        pipeline->bind("compressor.output_queue", "writer.input_queue");
        pipeline->noMoreBindings();

        auto pipeline_head = pipeline->getInPortQueue<Payload>("organizer.input_queue");
        collection_->openStage(pipeline_head);
    }

    /// \brief Run after initialization; invokes \ref CollectionPipelineMeta::writeMetaOnPostInit.
    void postInit(int, char**) override
    {
        collection_->writeMetaOnPostInit(db_mgr_);
    }

private:
    class AutoCollectorHandler
    {
    public:
        AutoCollectorHandler(size_t heartbeat, std::vector<char>&& bytes)
            : heartbeat_(heartbeat)
            , bytes_(std::move(bytes))
        {}

        void setBytes(std::vector<char>&& bytes)
        {
            bytes_ = std::move(bytes);
            counter_ = 0;
        }

        void appendToAutoCollection(std::vector<char>& auto_collected)
        {
            if (counter_++ % heartbeat_ == 0)
            {
                auto_collected.insert(auto_collected.end(), bytes_.begin(), bytes_.end());
            }
        }

    private:
        const size_t heartbeat_;
        size_t counter_ = 0;
        std::vector<char> bytes_;
    };

    class Organizer : public pipeline::Stage
    {
    public:
        Organizer(size_t heartbeat)
            : heartbeat_(heartbeat)
        {
            addInPort_<Payload>("input_queue", input_queue_);
            addOutPort_<Payload>("output_queue", output_queue_);
        }

    private:
        pipeline::PipelineAction run_(bool force) override
        {
            pipeline::PipelineAction action = pipeline::PipelineAction::SLEEP;
            Payload payload;
            while (input_queue_->try_pop(payload))
            {
                if (payload.auto_collected)
                {
                    const char* raw = payload.bytes.data();
                    const auto cid = *reinterpret_cast<const uint16_t*>(raw);
                    auto& handler = auto_collector_handlers_[cid];
                    if (!handler)
                    {
                        handler = std::make_unique<AutoCollectorHandler>(heartbeat_, std::move(payload.bytes));
                    }
                    else
                    {
                        handler->setBytes(std::move(payload.bytes));
                    }
                }

                // Try to buffer the incoming payload (only able to if it's for the same time point)
                if (!payload_queue_.empty() && payload_queue_.back().time_point->equals(payload.time_point.get(), true))
                {
                    auto& dst = payload_queue_.back().bytes;
                    const auto& src = payload.bytes;
                    dst.insert(dst.end(), src.begin(), src.end());
                }

                // Otherwise start a new entry in the queue. Flush first since we are
                // now done with that entry's time point (notice how we pass in 'true'
                // to the call to equals() above which guarantees time points never
                // go backwards).
                else
                {
                    flushToPipeline_(true /*leave newest*/);
                    payload_queue_.emplace(std::move(payload));
                }

                action = pipeline::PipelineAction::PROCEED;
            }

            if (force)
            {
                if (flushToPipeline_(false /*flush everything*/))
                {
                    action = pipeline::PipelineAction::PROCEED;
                }
            }

            return action;
        }

        bool flushToPipeline_(bool leave_newest)
        {
            auto flushed = false;
            auto target_size = leave_newest ? 1u : 0u;
            while (payload_queue_.size() > target_size)
            {
                auto payload = std::move(payload_queue_.front());
                for (const auto& [_, handler] : auto_collector_handlers_)
                {
                    handler->appendToAutoCollection(payload.bytes);
                }

                output_queue_->emplace(std::move(payload));
                payload_queue_.pop();
                flushed = true;
            }
            return flushed;
        }

        const size_t heartbeat_;
        ConcurrentQueue<Payload>* input_queue_ = nullptr;
        ConcurrentQueue<Payload>* output_queue_ = nullptr;
        std::queue<Payload> payload_queue_;
        std::unordered_map<uint16_t, std::unique_ptr<AutoCollectorHandler>> auto_collector_handlers_;
    };

    class Compressor : public pipeline::Stage
    {
    public:
        Compressor()
        {
            addInPort_<Payload>("input_queue", input_queue_);
            addOutPort_<Payload>("output_queue", output_queue_);
        }

    private:
        pipeline::PipelineAction run_(bool) override
        {
            Payload payload;
            if (input_queue_->try_pop(payload))
            {
                Payload compressed;
                compressed.time_point = payload.time_point;
                compressData(payload.bytes, compressed.bytes);
                output_queue_->emplace(std::move(compressed));
                return pipeline::PipelineAction::PROCEED;
            }

            return pipeline::PipelineAction::SLEEP;
        }

        ConcurrentQueue<Payload>* input_queue_ = nullptr;
        ConcurrentQueue<Payload>* output_queue_ = nullptr;
    };

    class Writer : public pipeline::DatabaseStage<CollectionPipeline>
    {
    public:
        Writer()
        {
            addInPort_<Payload>("input_queue", input_queue_);
        }

    private:
        pipeline::PipelineAction run_(bool) override
        {
            Payload payload;
            if (input_queue_->try_pop(payload))
            {
                /*auto inserter = getTableInserter_("Collection")
                auto inserter = getTableInserter_("TimedstampedData");
                payload.time_point->apply(inserter);
                inserter->setColumnValue(1, payload.bytes);
                inserter->createRecord();*/
                return pipeline::PipelineAction::PROCEED;
            }

            return pipeline::PipelineAction::SLEEP;
        }

        ConcurrentQueue<Payload>* input_queue_ = nullptr;
    };

    DatabaseManager *const db_mgr_;
    simdb::TinyStrings<> tiny_strings_;
    CollectionBase* collection_ = nullptr;
};

} // namespace simdb::collection
