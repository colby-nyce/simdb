/*
 * This test shows how to setup SimDB applications and the appropriate
 * API calls to make on the AppManager.
 */

// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/serialize/ThreadedSink.hpp"
#include "simdb/schema/Blob.hpp"
#include "simdb/test/SimDBTester.hpp"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <random>
#include <sstream>

TEST_INIT;

std::vector<char> generateRandomBytes(std::size_t num_bytes)
{
    std::vector<char> bytes(num_bytes);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    for (std::size_t i = 0; i < num_bytes; ++i) {
        bytes[i] = static_cast<char>(dis(gen));
    }

    return bytes;
}

class DummyApp : public simdb::App
{
public:
    static constexpr auto NAME = "DummyApp";
    static constexpr auto NUM_COMPRESSION_THREADS = 1;

    // App constructors must have this signature.
    DummyApp(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
        , sink_(db_mgr,
                END_OF_PIPELINE_CALLBACK(DummyApp, endOfPipeline_),
                NUM_COMPRESSION_THREADS)
    {
    }

    void appendSchema() override
    {
        simdb::Schema schema;
        using dt = simdb::SqlDataType;

        auto& meta_tbl = schema.addTable("Metadata");
        meta_tbl.addColumn("SimCmdline", dt::string_t);
        meta_tbl.addColumn("SimStartTime", dt::string_t);
        meta_tbl.addColumn("SimEndTime", dt::string_t);

        auto& blob_tbl = schema.addTable("BlobData");
        blob_tbl.addColumn("Tick", dt::int64_t);
        blob_tbl.addColumn("Data", dt::blob_t);

        db_mgr_->appendSchema(schema);
    }

    void preInit(int argc, char** argv) override
    {
        std::ostringstream oss;
        for (int i = 0; i < argc; ++i)
        {
            oss << argv[i];
            if (i < argc - 1)
            {
                oss << " ";
            }
        }

        sim_cmdline_ = oss.str();
    }

    void preSim() override
    {
        sim_start_time_ = getFormattedCurrentTime_();
    }

    void postSim() override
    {
        sim_end_time_ = getFormattedCurrentTime_();

        db_mgr_->INSERT(
            SQL_TABLE("Metadata"),
            SQL_COLUMNS("SimCmdline", "SimStartTime", "SimEndTime"),
            SQL_VALUES(sim_cmdline_, sim_start_time_, sim_end_time_));
    }

    void teardown() override
    {
        // If your app uses resources like ThreadedSink, now is the time to
        // call its teardown method.
        sink_.teardown();
    }

    void process(uint64_t tick, std::vector<char>&& data_bytes)
    {
        simdb::DatabaseEntry entry;
        entry.tick = tick;
        entry.data_ptr = data_bytes.data();
        entry.num_bytes = data_bytes.size();
        entry.container = data_bytes;
        sink_.push(std::move(entry));

        ++num_blobs_written_;
    }

    void validate()
    {
        validateMetadata_();
        validateBlobs_();
    }

private:
    std::string getFormattedCurrentTime_() const
    {
        // Get current time as time_point
        auto now = std::chrono::system_clock::now();

        // Convert to time_t for formatting
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);

        // Convert to local time
        std::tm* local_time = std::localtime(&now_c);

        // Format as MM::DD::YYYY hh::mm::ss
        std::ostringstream oss;
        oss << std::put_time(local_time, "%m::%d::%Y %H::%M::%S");
        return oss.str();
    }

    void endOfPipeline_(simdb::DatabaseManager* db_mgr,
                        simdb::DatabaseEntry&& entry)
    {
        // We are using 1 compression thread, so make sure it was compressed.
        if (!entry.compressed)
        {
            throw simdb::DBException("Data was not compressed in DummyApp.");
        }

        simdb::SqlBlob blob;
        blob.data_ptr = entry.data_ptr;
        blob.num_bytes = entry.num_bytes;

        db_mgr->INSERT(
            SQL_TABLE("BlobData"),
            SQL_COLUMNS("Tick", "Data"),
            SQL_VALUES(entry.tick, blob));
    }

    void validateMetadata_()
    {
        auto meta_query = db_mgr_->createQuery("Metadata");

        std::string sim_cmdline;
        meta_query->select("SimCmdline", sim_cmdline);

        std::string sim_start_time;
        meta_query->select("SimStartTime", sim_start_time);

        std::string sim_end_time;
        meta_query->select("SimEndTime", sim_end_time);

        auto result = meta_query->getResultSet();
        EXPECT_TRUE(result.getNextRecord());
        EXPECT_EQUAL(sim_cmdline, sim_cmdline_);
        EXPECT_EQUAL(sim_start_time, sim_start_time_);
        EXPECT_EQUAL(sim_end_time, sim_end_time_);
    }

    void validateBlobs_()
    {
        auto blob_query = db_mgr_->createQuery("BlobData");
        EXPECT_EQUAL(blob_query->count(), num_blobs_written_);
    }

    simdb::DatabaseManager* db_mgr_;
    std::string sim_cmdline_;
    std::string sim_start_time_;
    std::string sim_end_time_;
    simdb::ThreadedSink sink_;
    size_t num_blobs_written_ = 0;
};

REGISTER_SIMDB_APPLICATION(DummyApp);

int main(int argc, char** argv)
{
    DB_INIT;

    auto& app_mgr = simdb::AppManager::getInstance();
    app_mgr.enableApp(DummyApp::NAME);
    EXPECT_TRUE(app_mgr.enabled(DummyApp::NAME));

    simdb::DatabaseManager db_mgr("test.db");

    // Setup...
    app_mgr.createEnabledApps(&db_mgr);
    app_mgr.createSchemas(&db_mgr);
    app_mgr.preInit(&db_mgr, argc, argv);
    app_mgr.preSim(&db_mgr);

    // Simulate...
    auto dummy_app = app_mgr.getApp<DummyApp>();
    for (uint64_t tick = 0; tick < 1000; ++tick)
    {
        auto data_bytes = generateRandomBytes(256);
        dummy_app->process(tick, std::move(data_bytes));
    }

    // Finish...
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown(&db_mgr);

    // Validate...
    dummy_app->validate();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
