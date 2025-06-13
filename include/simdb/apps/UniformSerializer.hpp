#pragma once

#include "simdb/apps/PipelineApp.hpp"

namespace simdb
{

/// This is a base class for SimDB applications that want to send
/// raw data through a pipeline for processing. The difference
/// between using PipelineApp and this class is that the "uniform"
/// serializer is used to process data with a well-defined (and
/// unchanging) byte layout. This is useful when you want to
/// collect data once from simulation, and post-process it later
/// from python export scripts or other tools (the byte layout
/// is defined by subclasses and this base class writes it to
/// the database).

class UniformSerializer : public PipelineApp
{
public:
    UniformSerializer() = default;

    bool defineSchema(simdb::Schema& schema) override final
    {
        using dt = SqlDataType;

        auto& blob_tbl = schema.addTable("UnifiedCollectorBlobs");
        blob_tbl.addColumn("AppID", dt::int32_t);
        blob_tbl.addColumn("Tick", dt::int64_t);
        blob_tbl.addColumn("DataBlob", dt::blob_t);
        blob_tbl.addColumn("IsCompressed", dt::int32_t);
        blob_tbl.createCompoundIndexOn({"AppID", "Tick"});

        auto& meta_tbl = schema.addTable("UnifiedCollectorByteLayouts");
        meta_tbl.addColumn("AppID", dt::int32_t);
        meta_tbl.addColumn("ByteLayoutYAML", dt::string_t);

        defineSchema_(schema);
        return true;
    }

    void postInit(int argc, char** argv) override final
    {
        auto db_mgr = getDatabaseManager();

        db_mgr->INSERT(
            SQL_TABLE("UnifiedCollectorByteLayouts"),
            SQL_COLUMNS("AppID", "ByteLayoutYAML"),
            SQL_VALUES(getAppID_(), getByteLayoutYAML_()));

        postInit_(argc, argv);
    }

    void postSim() override final
    {
        postSim_();
    }

protected:
    static void CompressEntry(PipelineEntry& entry)
    {
        entry.compress();
    }

    static void CommitEntry(PipelineEntry& entry)
    {
        auto db_mgr = entry.getDatabaseManager();

        auto record = db_mgr->INSERT(
            SQL_TABLE("UnifiedCollectorBlobs"),
            SQL_COLUMNS("AppID", "Tick", "DataBlob", "IsCompressed"),
            SQL_VALUES(entry.getAppId(), entry.getTick(), entry.getBlob(), entry.compressed()));

        entry.setCommittedDbId(record->getId());
    }

private:
    virtual void defineSchema_(Schema&) {}

    virtual void postInit_(int argc, char** argv)
    {
        (void)argc;   // Unused parameter
        (void)argv;   // Unused parameter
    }

    virtual void postSim_() {}

    virtual void onPreTeardown_() {}

    virtual void onPostTeardown_() {}

    virtual std::string getByteLayoutYAML_() const = 0;
};

} // namespace simdb
