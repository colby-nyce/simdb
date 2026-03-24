// <CollectionPipeline.hpp> -*- C++ *-*

#pragma once

#include "simdb/apps/App.hpp"

namespace simdb::collection {

/// \class CollectionPipelineMeta
/// \brief Callback interface for collection lifecycle tied to a \ref CollectionPipeline app.
///
/// Implementations run while the pipeline app is active: after \ref App postInit, before preTeardown,
/// and after postTeardown. Use these hooks to build trees, register collectables, or flush state to
/// \a db_mgr.
class CollectionPipelineMeta
{
public:
    virtual ~CollectionPipelineMeta() = default;

    /// \brief Called when collection starts (pipeline \ref App postInit).
    /// \param db_mgr Database manager for the current run.
    virtual void onCollectionStarting(DatabaseManager* db_mgr) = 0;

    /// \brief Called when collection is stopping (pipeline \ref App preTeardown).
    /// \param db_mgr Database manager for the current run.
    virtual void onCollectionStopping(DatabaseManager* db_mgr) = 0;

    /// \brief Called after collection has stopped (pipeline \ref App postTeardown).
    /// \param db_mgr Database manager for the current run.
    virtual void onCollectionStopped(DatabaseManager* db_mgr) = 0;
};

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
    CollectionPipeline(DatabaseManager* db_mgr, CollectionPipelineMeta* handler)
        : db_mgr_(db_mgr)
        , tiny_strings_(db_mgr)
        , meta_handler_(handler)
    {}

    /// \class AppFactory
    /// \brief \ref AppFactoryBase that creates \ref CollectionPipeline instances sharing a configured \ref CollectionPipelineMeta.
    class AppFactory : public AppFactoryBase
    {
    public:
        using AppT = CollectionPipeline;

        /// \brief Set the meta handler passed to every \ref createApp result.
        /// \param handler Implementation receiving collection lifecycle callbacks; must not be null when \ref createApp is called.
        void parameterize(CollectionPipelineMeta* handler)
        {
            meta_handler_ = handler;
        }

        /// \brief Allocate a new pipeline app bound to the parameterized meta handler.
        /// \param db_mgr Database manager for the new app.
        /// \return New \ref CollectionPipeline; ownership follows the app framework contract.
        /// \note Debug builds assert if \ref meta_handler_ was never set via \ref parameterize.
        AppT* createApp(DatabaseManager* db_mgr) override
        {
            assert(meta_handler_ != nullptr);
            return new AppT(db_mgr, meta_handler_);
        }

        /// \brief Populate \a schema with the same tables as \ref CollectionPipeline::defineSchema.
        /// \param schema Schema to fill before opening the database.
        void defineSchema(Schema& schema) const override
        {
            AppT::defineSchema(schema);
        }

    private:
        /// \brief Handler wired into each created app; set by \ref parameterize.
        CollectionPipelineMeta* meta_handler_ = nullptr;
    };

    /// \brief Declare SQLite tables used by Argos collection (globals, clocks, element/collectable trees, dtypes, structs, enums, string map, records, queue sizes).
    /// \param schema Schema object to extend.
    static void defineSchema(Schema& schema)
    {
        using dt = SqlDataType;

        auto& globals_tbl = schema.addTable("CollectionGlobals");
        globals_tbl.addColumn("Heartbeat", dt::int32_t);

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

        auto& collected_dtypes_tbl = schema.addTable("CollectedDataTypes");
        collected_dtypes_tbl.addColumn("DemangledName", dt::string_t);
        collected_dtypes_tbl.addColumn("NumBytes", dt::int32_t);

        //TODO cnyce: add tables that are designed better for queues, e.g.
        //capacity and sparse flags

        auto& struct_fields_tbl = schema.addTable("StructFields");
        struct_fields_tbl.addColumn("StructName", dt::string_t);
        struct_fields_tbl.addColumn("FieldName", dt::string_t);
        struct_fields_tbl.addColumn("FieldType", dt::string_t);
        struct_fields_tbl.addColumn("FormatCode", dt::int32_t);
        struct_fields_tbl.addColumn("IsAutoColorizeKey", dt::int32_t);
        struct_fields_tbl.addColumn("IsDisplayedByDefault", dt::int32_t);
        struct_fields_tbl.setColumnDefaultValue("IsAutoColorizeKey", 0);
        struct_fields_tbl.setColumnDefaultValue("IsDisplayedByDefault", 1);

        auto& enums_tbl = schema.addTable("Enums");
        enums_tbl.addColumn("EnumName", dt::string_t);
        enums_tbl.addColumn("IntType", dt::string_t);

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

    /// \brief Return the string intern table used when mapping string values to stable integer IDs for storage.
    /// \return Pointer to the owned \ref TinyStrings instance (valid for the app lifetime).
    TinyStrings<>* getTinyStrings()
    {
        return &tiny_strings_;
    }

    /// \brief Run after initialization; invokes \ref CollectionPipelineMeta::onCollectionStarting.
    void postInit(int, char**) override
    {
        meta_handler_->onCollectionStarting(db_mgr_);
    }

    /// \brief Run before teardown; invokes \ref CollectionPipelineMeta::onCollectionStopping.
    void preTeardown() override
    {
        meta_handler_->onCollectionStopping(db_mgr_);
    }

    /// \brief Run after teardown; invokes \ref CollectionPipelineMeta::onCollectionStopped.
    void postTeardown() override
    {
        meta_handler_->onCollectionStopped(db_mgr_);
    }

private:
    DatabaseManager *const db_mgr_;
    TinyStrings<> tiny_strings_;
    CollectionPipelineMeta* meta_handler_ = nullptr;
};

} // namespace simdb::collection
