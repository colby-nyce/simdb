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
#include "simdb/pipeline/AsyncPipeline.hpp"
#include "simdb/schema/Blob.hpp"

namespace simdb {

class UnifiedSerializer : public App
{
public:
    // Note that we do not provide a name for this app, as it is not intended to be
    // run directly. Instead, it is used as a base class for other applications that
    // want to use a unified std::vector<char> collector with a specific byte layout.

    UnifiedSerializer(DatabaseManager* db_mgr, size_t num_compression_threads)
        : db_mgr_(db_mgr)
        , compression_enabled_(num_compression_threads > 0)
        , sink_(END_OF_PIPELINE_CALLBACK(UnifiedSerializer, endOfPipeline_),
                num_compression_threads)
    {
    }

    virtual ~UnifiedSerializer() = default;

    void appendSchema() override final
    {
        Schema schema;
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
        setCompressionLevel_(CompressionLevel::FASTEST);
    }

    void useHighestCompression()
    {
        setCompressionLevel_(CompressionLevel::HIGHEST);
    }

    void disableCompression()
    {
        setCompressionLevel_(CompressionLevel::DISABLED);
    }

    template <typename T>
    void process(uint64_t tick, const std::vector<T>& data)
    {
        process_(tick, data.data(), data.size() * sizeof(T), data);
    }

    template <typename T>
    void process(uint64_t tick, std::vector<T>&& data)
    {
        process_(tick, data.data(), data.size() * sizeof(T), std::move(data));
    }

    template <typename T, size_t N>
    void process(uint64_t tick, const std::array<T, N>& data)
    {
        process_(tick, data.data(), data.size() * sizeof(T), data);
    }

    template <typename T, size_t N>
    void process(uint64_t tick, std::array<T, N>&& data)
    {
        process_(tick, data.data(), data.size() * sizeof(T), std::move(data));
    }

private:
    template <typename BytesContainer>
    void process_(uint64_t tick, const void* data_ptr, size_t num_bytes, const BytesContainer& container)
    {
        DatabaseEntry entry;
        entry.db_mgr = db_mgr_;
        entry.tick = tick;
        entry.data_ptr = data_ptr;
        entry.num_bytes = num_bytes;
        entry.container = container;
        sink_.process(std::move(entry));
    }

    template <typename BytesContainer>
    void process_(uint64_t tick, const void* data_ptr, size_t num_bytes, BytesContainer&& container)
    {
        DatabaseEntry entry;
        entry.db_mgr = db_mgr_;
        entry.tick = tick;
        entry.data_ptr = data_ptr;
        entry.num_bytes = num_bytes;
        entry.container = std::move(container);
        sink_.process(std::move(entry));
    }

    void endOfPipeline_(DatabaseEntry&& entry)
    {
        SqlBlob blob;
        blob.data_ptr = entry.data_ptr;
        blob.num_bytes = entry.num_bytes;

        // Sanity check that the DatabaseEntry has our DatabaseManager.
        if (entry.db_mgr != db_mgr_)
        {
            throw DBException("DatabaseEntry's db_mgr does not match UnifiedSerializer's db_mgr.");
        }

        db_mgr_->INSERT(
            SQL_TABLE("UnifiedCollectorBlobs"),
            SQL_COLUMNS("AppID", "Tick", "DataBlob", "IsCompressed"),
            SQL_VALUES(getAppID_(), entry.tick, blob, entry.compressed));
    }

    void setCompressionLevel_(CompressionLevel level)
    {
        if (!compression_enabled_ && level != CompressionLevel::DISABLED)
        {
            throw DBException("Compression is disabled for this application.");
        }
        sink_.setCompressionLevel(level);
    }

    virtual void appendSchema_(DatabaseManager*, Schema&) {}

    virtual void preInit_(DatabaseManager*, int argc, char** argv)
    {
        (void)argc;   // Unused parameter
        (void)argv;   // Unused parameter
    }

    virtual void preSim_(DatabaseManager*) {}

    virtual void postSim_(DatabaseManager*) {}

    virtual void postTeardown_(DatabaseManager*) {}

    virtual std::string getByteLayoutYAML_() const = 0;

    DatabaseManager* db_mgr_;
    bool compression_enabled_;
    AsyncPipeline sink_;
};

} // namespace simdb
