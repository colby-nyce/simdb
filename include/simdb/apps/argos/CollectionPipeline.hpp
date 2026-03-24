// <CollectionPipeline.hpp> -*- C++ *-*

#pragma once

#include "simdb/apps/App.hpp"

namespace simdb::collection {

/// TODO cnyce
class CollectionPipelineMeta
{
public:
    virtual ~CollectionPipelineMeta() = default;
    virtual void onCollectionStarting(DatabaseManager* db_mgr) = 0;
    virtual void onCollectionStopping(DatabaseManager* db_mgr) = 0;
    virtual void onCollectionStopped(DatabaseManager* db_mgr) = 0;
};

/// TODO cnyce
class CollectionPipeline : public App
{
public:
    static constexpr auto NAME = "collection-pipeline";

    /// TODO cnyce
    CollectionPipeline(DatabaseManager* db_mgr, CollectionPipelineMeta* handler)
        : db_mgr_(db_mgr)
        , tiny_strings_(db_mgr)
        , meta_handler_(handler)
    {}

    /// TODO cnyce
    class AppFactory : public AppFactoryBase
    {
    public:
        using AppT = CollectionPipeline;

        /// TODO cnyce
        void parameterize(CollectionPipelineMeta* handler)
        {
            meta_handler_ = handler;
        }

        /// TODO cnyce
        AppT* createApp(DatabaseManager* db_mgr) override
        {
            assert(meta_handler_ != nullptr);
            return new AppT(db_mgr, meta_handler_);
        }

        /// TODO cnyce
        void defineSchema(Schema& schema) const override
        {
            AppT::defineSchema(schema);
        }

    private:
        /// TODO cnyce
        CollectionPipelineMeta* meta_handler_ = nullptr;
    };

    /// TODO cnyce
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

    /// TODO cnyce
    TinyStrings<>* getTinyStrings()
    {
        return &tiny_strings_;
    }

    /// TODO cnyce
    void postInit(int, char**) override
    {
        meta_handler_->onCollectionStarting(db_mgr_);
    }

    /// TODO cnyce
    void preTeardown() override
    {
        meta_handler_->onCollectionStopping(db_mgr_);
    }

    /// TODO cnyce
    void postTeardown() override
    {
        meta_handler_->onCollectionStopped(db_mgr_);
    }

private:
    DatabaseManager *const db_mgr_;
    TinyStrings<> tiny_strings_;
    CollectionPipelineMeta* meta_handler_ = nullptr;
};

} // namespace simdb::collections
