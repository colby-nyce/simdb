// <CollectionPipeline.hpp> -*- C++ *-*

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/utils/TinyStrings.hpp"

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

    /// \brief Declare SQLite tables used by Argos collection (globals, clocks, element/collectable trees,
    /// data-type metadata, string map, records, queue sizes).
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
    simdb::TinyStrings<>* getTinyStrings()
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
    simdb::TinyStrings<> tiny_strings_;
    CollectionPipelineMeta* meta_handler_ = nullptr;
};

} // namespace simdb::collection
