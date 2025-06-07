// <UnifiedSerializer.hpp> -*- C++ -*-

#pragma once

/// The UnifiedSerializer app is used to collect raw data with a provided
/// byte layout, making it easy to collect arbitrary high-volume data
/// during simulation in a performant way. SimDB's python exporter module
/// can then be used to create user-defined post-processing tools to unpack
/// the raw data into formatted python objects. For example, you could have
/// exporters that:
///
///   1. Export a range of data (tick1 -> tick2) to a CSV file.
///   2. Gather summary data (e.g. mean values, min/max, final stat values, etc.)
///      and write to a JSON file.
///   3. Convert SQLite database records into an HDF5 file.
///   4. Provide a web-based viewer for the data.
///   5. ... and so on.

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/serialize/ThreadedSink.hpp"

namespace simdb {

template <typename RawDataT = char>
class UnifiedSerializer : public simdb::App
{
public:
    // Note that we do not provide a name for this app, as it is not intended to be
    // run directly. Instead, it is used as a base class for other applications that
    // want to use a unified std::vector<char> collector with a specific byte layout.

    UnifiedSerializer(simdb::DatabaseManager* db_mgr, bool compression_enabled = true)
        : db_mgr_(db_mgr)
        , compression_enabled_(compression_enabled)
        , sink_(db_mgr,
                END_OF_PIPELINE_CALLBACK(UnifiedSerializer, endOfPipeline_),
                compression_enabled ? 1 : 0)
    {
    }

    virtual ~UnifiedSerializer() = default;

    void appendSchema() override final
    {
        simdb::Schema schema;
        using dt = simdb::SqlDataType;

        auto& blob_tbl = schema.addTable("UnifiedCollectorBlobs");
        blob_tbl.addColumn("AppID", dt::int32_t);
        blob_tbl.addColumn("Tick", dt::int64_t);
        blob_tbl.addColumn("DataBlob", dt::blob_t);
        blob_tbl.addColumn("IsCompressed", dt::int32_t);
        blob_tbl.createCompoundIndexOn({"AppID", "Tick"});

        auto& meta_tbl = schema.addTable("UnifiedCollectorByteLayouts");
        meta_tbl.addColumn("AppID", dt::int32_t);
        meta_tbl.addColumn("ByteLayoutYAML", dt::string_t);

        appendSchema_(db_mgr_, schema);
        db_mgr_->appendSchema(schema);
    }

    void preInit(int argc, char** argv) override final
    {
        preInit_(db_mgr_, argc, argv);
    }

    void preSim() override final
    {
        db_mgr_->INSERT(
            SQL_TABLE("UnifiedCollectorByteLayouts"),
            SQL_COLUMNS("AppID", "ByteLayoutYAML"),
            SQL_VALUES(getAppID_(), getByteLayoutYAML_()));

        preSim_(db_mgr_);
    }

    void postSim() override final
    {
        postSim_(db_mgr_);
    }

    void teardown() override final
    {
        sink_.teardown();
        postTeardown_(db_mgr_);
    }

    void useFastestCompression()
    {
        setCompressionLevel_(simdb::CompressionLevel::FASTEST);
    }

    void useHighestCompression()
    {
        setCompressionLevel_(simdb::CompressionLevel::HIGHEST);
    }

    void disableCompression()
    {
        setCompressionLevel_(simdb::CompressionLevel::DISABLED);
    }

    template <typename T=char>
    void process(uint64_t tick, const std::vector<T>& data)
    {
        simdb::DatabaseEntry<T> entry;
        entry.tick = tick;
        entry.bytes = data;
        sink_.push(std::move(entry));
    }

    template <typename T=char>
    void process(uint64_t tick, std::vector<T>&& data)
    {
        simdb::DatabaseEntry<T> entry;
        entry.tick = tick;
        entry.bytes = std::move(data);
        sink_.push(std::move(entry));
    }

private:
    void endOfPipeline_(simdb::DatabaseManager* db_mgr,
                        simdb::DatabaseEntry<RawDataT>&& entry)
    {
        db_mgr->INSERT(
            SQL_TABLE("UnifiedCollectorBlobs"),
            SQL_COLUMNS("AppID", "Tick", "DataBlob", "IsCompressed"),
            SQL_VALUES(getAppID_(), entry.tick, entry.bytes, entry.compressed));
    }

    void setCompressionLevel_(simdb::CompressionLevel level)
    {
        if (!compression_enabled_ && level != simdb::CompressionLevel::DISABLED)
        {
            throw simdb::DBException("Compression is disabled for this application.");
        }
        sink_.setCompressionLevel(level);
    }

    virtual void appendSchema_(simdb::DatabaseManager*, simdb::Schema&) {}

    virtual void preInit_(simdb::DatabaseManager*, int argc, char** argv)
    {
        (void)argc;   // Unused parameter
        (void)argv;   // Unused parameter
    }

    virtual void preSim_(simdb::DatabaseManager*) {}

    virtual void postSim_(simdb::DatabaseManager*) {}

    virtual void postTeardown_(simdb::DatabaseManager*) {}

    virtual std::string getByteLayoutYAML_() const = 0;

    simdb::DatabaseManager* db_mgr_;
    bool compression_enabled_;
    simdb::ThreadedSink<RawDataT> sink_;
};

} // namespace simdb
