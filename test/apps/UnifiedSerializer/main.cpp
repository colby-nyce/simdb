/// Test SimDB's "Unified Serializer" feature. This feature is all about making
/// it easy to define byte layouts, collect raw bytes during simulation, and use
/// a registered python exporter class to export the raw data to the python caller
/// for further processing.
///
/// Examples:
///   1. Exporting a range of data (tick1 -> tick2) to a CSV file.
///   2. Gather summary data (e.g. mean values, min/max, final stat values, etc.)
///      and write to a JSON file.
///   3. Converting SQLite database records into an HDF5 file.

// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/serialize/ThreadedSink.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

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
        blob_tbl.createCompoundIndexOn(SQL_COLUMNS("AppID", "Tick"));

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

    void process(uint64_t tick, const std::vector<char>& data)
    {
        simdb::DatabaseEntry entry;
        entry.tick = tick;
        entry.bytes = data;
        sink_.push(std::move(entry));
    }

    void process(uint64_t tick, std::vector<char>&& data)
    {
        simdb::DatabaseEntry entry;
        entry.tick = tick;
        entry.bytes = std::move(data);
        sink_.push(std::move(entry));
    }

private:
    void endOfPipeline_(simdb::DatabaseManager* db_mgr,
                        simdb::DatabaseEntry&& entry)
    {
        db_mgr->INSERT(
            SQL_TABLE("UnifiedCollectorBlobs"),
            SQL_COLUMNS("AppID", "Tick", "DataBlob", "IsCompressed"),
            SQL_VALUES(getAppID_(), entry.tick, std::move(entry.bytes), entry.compressed));
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
    simdb::ThreadedSink<> sink_;
};

// Here is a specific use case of the UnifiedSerializer that collects
// double-value statistics in a flat list. We will use metadata tables
// to determine the statistic name at each position in the list (vector).
//
// The requested python exporter will deserialize as raw bytes, decompress
// the blobs, convert to a list of floating-point values, and do something
// with them (e.g. write to a CSV file, display in a web page, etc).
//
// Note that we can run simulation once to get the data, and have multiple
// python classes handle the data in different ways, e.g.:
//
//   1.  Run simulation to get the database file (assume "sim.db")
//   2a. simdb_export --exporter summary-stats --outfile summary.json sim.db
//   2b. simdb_export --exporter csv-export --outfile data.csv sim.db
//   2c. simdb_export --exporter hdf5-export --outfile data.h5 sim.db
//   2d. simdb_export --exporter stats-viewer sim.db
//
//   Or you could design a reusable exporter like this:
//
//   1.  Run simulation to get the database file (assume "sim.db")
//   2a. simdb_export --exporter stats-report --outfile summary.json --format json sim.db
//   2b. simdb_export --exporter stats-report --outfile data.csv --format csv sim.db
//   2c. simdb_export --exporter stats-report --outfile data.h5 --format hdf5 sim.db
//   2d. simdb_export --exporter stats-report --format html --browser /usr/bin/firefox sim.db

class StatsCollector : public UnifiedSerializer
{
public:
    static constexpr auto NAME = "StatsCollector";

    StatsCollector(simdb::DatabaseManager* db_mgr)
        : UnifiedSerializer(db_mgr, true)
    {
    }

    void appendStat(const std::string& name)
    {
        if (finalized_)
        {
            throw simdb::DBException("StatsCollector: Cannot append stats after preSim() has been called.");
        }
        stat_names_.push_back(name);
    }

    using UnifiedSerializer::process;

    void process(uint64_t tick, const std::vector<double>& stats)
    {
        if (stats.size() != stat_names_.size())
        {
            throw simdb::DBException("StatsCollector: Mismatched stats size.");
        }

        std::vector<char> data;
        data.reserve(stats.size() * sizeof(double));
        for (const auto& stat : stats)
        {
            data.insert(data.end(), reinterpret_cast<const char*>(&stat),
                        reinterpret_cast<const char*>(&stat) + sizeof(double));
        }

        process(tick, std::move(data));
    }

private:
    void appendSchema_(simdb::DatabaseManager* db_mgr, simdb::Schema& schema) override final
    {
        using dt = simdb::SqlDataType;

        auto& stats_tbl = schema.addTable("StatsCollectorStatNames");
        stats_tbl.addColumn("AppID", dt::int32_t);
        stats_tbl.addColumn("StatName", dt::string_t);
        stats_tbl.createIndexOn("AppID");
    }

    void preSim_(simdb::DatabaseManager* db_mgr) override final
    {
        for (const auto& name : stat_names_)
        {
            db_mgr->INSERT(
                SQL_TABLE("StatsCollectorStatNames"),
                SQL_COLUMNS("AppID", "StatName"),
                SQL_VALUES(getAppID_(), name));
        }

        finalized_ = true;
    }

    std::string getByteLayoutYAML_() const override final
    {
        // This is a simple YAML representation of the byte layout.
        // These should be YAML lists with DType, Len, and Name fields.
        // In this case, it will look like:
        //
        // - DType: double
        //   Len:   <how many stats we have>
        //   Name:  stat_values

        std::ostringstream oss;
        oss << "- DType: double\n";
        oss << "  Len: " << stat_names_.size() << "\n";
        oss << "  Name: stat_values\n";
        return oss.str();
    }

    std::vector<std::string> stat_names_;
    bool finalized_ = false;
};

REGISTER_SIMDB_APPLICATION(StatsCollector);

std::vector<double> generateRandomStats(size_t count)
{
    std::vector<double> stats;
    stats.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        stats.push_back(rand() % 1000 / 3.14159);
    }
    return stats;
}

int main(int argc, char** argv)
{
    DB_INIT;

    auto& app_mgr = simdb::AppManager::getInstance();
    app_mgr.enableApp(StatsCollector::NAME);
    EXPECT_TRUE(app_mgr.enabled(StatsCollector::NAME));

    simdb::DatabaseManager db_mgr("test.db");

    // Setup...
    app_mgr.createEnabledApps(&db_mgr);
    auto stats_collector = app_mgr.getApp<StatsCollector>();

    stats_collector->appendStat("Foo");
    stats_collector->appendStat("Bar");
    stats_collector->appendStat("Fiz");
    stats_collector->appendStat("Buz");

    app_mgr.createSchemas(&db_mgr);
    app_mgr.preInit(&db_mgr, argc, argv);
    app_mgr.preSim(&db_mgr);

    // Cannot add more stats at this point.
    EXPECT_THROW(stats_collector->appendStat("Nope"));

    // Simulate...
    for (uint64_t tick = 0; tick < 1000; ++tick)
    {
        auto stats = generateRandomStats(4);
        stats_collector->process(tick, std::move(stats));
    }

    // Finish...
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown(&db_mgr);

    // This MUST be put at the end of unit test files' main() function.
    ENSURE_ALL_REACHED(0);
    REPORT_ERROR;
    return ERROR_CODE;
}
