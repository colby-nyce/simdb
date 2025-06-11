#include "simdb/apps/AppRegistration.hpp"
#include "simdb/apps/PipelineApp.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <iostream>
#include <random>

// clang-format off

/// This test uses the SimDB pipeline framework to demonstrate how to
/// create a pipeline application that processes data entries through
/// a series of stages. Each stage can be customized to perform specific
/// operations on the data, such as compression, serialization, or
/// transformation.

using simdb::PipelineChain;
using simdb::PipelineEntry;

std::vector<double> generateRandomData(size_t size)
{
    std::vector<double> data(size);
    std::mt19937 gen(std::random_device{}());
    std::uniform_real_distribution<double> dis(0.0, 100.0);
    
    for (auto& value : data)
    {
        value = dis(gen);
    }
    
    return data;
}

static void UpdateCalledChainLinks(PipelineEntry& entry, const std::string& func_name)
{
    auto db_mgr = entry.getDatabaseManager();
    auto tick = entry.getTick();
    auto compressed = entry.compressed() ? 1 : 0;
    auto db_id = entry.getCommittedDbID();

    auto record = db_mgr->INSERT(
        SQL_TABLE("CalledChainLinks"),
        SQL_COLUMNS("Tick", "FunctionName", "EntryCompressed", "EntryDbId"),
        SQL_VALUES(tick, func_name, compressed, db_id));

    if (!db_id)
    {
        entry.setCommittedDbId(record->getId());
    }
}

static void ProcessCollectorEntry(PipelineEntry& entry)
{
    UpdateCalledChainLinks(entry, "ProcessCollectorEntry");
}

static void ProcessStatsCollectorEntry(PipelineEntry& entry)
{
    UpdateCalledChainLinks(entry, "ProcessStatsCollectorEntry");
}

void DoExtraStuff(PipelineEntry& entry)
{
    UpdateCalledChainLinks(entry, "DoExtraStuff");
}

// ------------------------------------------------------------------------
class Collector : public simdb::PipelineApp
{
public:
    Collector(simdb::AppPipeline& pipeline, PipelineChain serialization_chain = PipelineChain())
        : simdb::PipelineApp(pipeline, serialization_chain + ProcessCollectorEntry)
    {
    }
};

// ------------------------------------------------------------------------
class StatsCollector : public Collector
{
public:
    static constexpr auto NAME = "StatsCollector";

    StatsCollector(simdb::AppPipeline& pipeline, PipelineChain serialization_chain = PipelineChain())
        : Collector(pipeline, serialization_chain + ProcessStatsCollectorEntry)
    {
    }

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;

        // This table is used to store app-specific metadata
        auto& meta_tbl = schema.addTable("Metadata");
        meta_tbl.addColumn("SimCmdline", dt::string_t);
        meta_tbl.addColumn("SimStartTime", dt::string_t);
        meta_tbl.addColumn("SimEndTime", dt::string_t);

        // This table is used to verify the serialization PipelineChain
        auto& funcs_tbl = schema.addTable("CalledChainLinks");
        funcs_tbl.addColumn("Tick", dt::int64_t);
        funcs_tbl.addColumn("FunctionName", dt::string_t);
        funcs_tbl.addColumn("EntryCompressed", dt::int32_t);
        funcs_tbl.addColumn("EntryDbId", dt::int32_t);

        // This table is used to store the raw data
        auto& blob_tbl = schema.addTable("BlobData");
        blob_tbl.addColumn("Tick", dt::int64_t);
        blob_tbl.addColumn("Data", dt::blob_t);

        // Make the verification step faster
        funcs_tbl.createCompoundIndexOn({"Tick", "FunctionName"});

        return true;
    }

    void postInit(int argc, char** argv) override
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
        sim_start_time_ = getFormattedCurrentTime_();
    }

    void postSim() override
    {
        sim_end_time_ = getFormattedCurrentTime_();
        auto db_mgr = getDatabaseManager();

        db_mgr->INSERT(
            SQL_TABLE("Metadata"),
            SQL_COLUMNS("SimCmdline", "SimStartTime", "SimEndTime"),
            SQL_VALUES(sim_cmdline_, sim_start_time_, sim_end_time_));
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

    std::string sim_cmdline_;
    std::string sim_start_time_;
    std::string sim_end_time_;
};

REGISTER_SIMDB_APPLICATION(StatsCollector);

// ------------------------------------------------------------------------
int main(int argc, char** argv)
{
    DB_INIT;

    auto& app_mgr = simdb::AppManager::getInstance();
    app_mgr.enableApp(StatsCollector::NAME);

    simdb::DatabaseManager db_mgr("test.db");

    // Setup...
    app_mgr.createEnabledApps(&db_mgr);
    app_mgr.createSchemas(&db_mgr);
    app_mgr.postInit(&db_mgr, argc, argv);

    // Simulate...
    auto pipeline_collector = app_mgr.getApp<StatsCollector>(&db_mgr);
    constexpr auto NUM_TICKS = 100;
    for (int tick = 0; tick < NUM_TICKS; ++tick)
    {
        // Since we have a std::vector<double> of data and the PipelineEntry
        // only uses std::vector<char>, we can use a utility from the PipelineApp
        // to convert our vector to a char vector. Converting data through the
        // PipelineApp has a performance advantage of using a pool of char
        // vectors under the hood to prevent unnecessary allocations.
        simdb::VectorSerializer<double> vector =
            pipeline_collector->createVectorSerializer<double>();

        vector = generateRandomData(1000);

        // Process the data and tell the pipeline to append DoExtraStuff()
        // to the serialization chain. On the database write thread, these
        // functions will get called:
        //
        //   ProcessCollectorEntry()          |-- These came from the PipelineApp
        //   ProcessStatsCollectorEntry()  |-- class hierarchy and always process
        //                                    |-- from the base class down.
        //   DoExtraStuff()
        pipeline_collector->process(tick, std::move(vector), DoExtraStuff);
    }

    // Finish...
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown();
    app_mgr.deleteApps();

    // Validate...
    auto query = db_mgr.createQuery("CalledChainLinks");

    std::string func_name;
    query->select("FunctionName", func_name);

    int32_t entry_compressed;
    query->select("EntryCompressed", entry_compressed);

    int32_t entry_db_id;
    query->select("EntryDbId", entry_db_id);

    for (int tick = 0; tick < NUM_TICKS; ++tick)
    {
        query->resetConstraints();
        query->addConstraintForInt("Tick", simdb::Constraints::EQUAL, tick);

        // First verify that exactly 3 functions were called
        // in the serialization chain.
        EXPECT_EQUAL(query->count(), 3);

        // Now verify that the functions were called in the
        // correct order.
        const char* expected_funcs[] = {
            "ProcessCollectorEntry",
            "ProcessStatsCollectorEntry",
            "DoExtraStuff"
        };

        auto results = query->getResultSet();
        size_t loop_idx = 0;
        int db_id = 0;
        while (results.getNextRecord())
        {
            EXPECT_EQUAL(func_name, expected_funcs[loop_idx]);

            // Verify that the entry was compressed by the
            // time it got to the serialization stage.
            EXPECT_EQUAL(entry_compressed, 1);

            // Verify that the first serialization callback
            // set the committed database ID. All other
            // functions in the chain should have the same
            // database ID for this tick.
            if (entry_db_id)
            {
                db_id = entry_db_id;
            }

            // Update the loop index for the next iteration.
            if (loop_idx == 2)
            {
                db_id = 0;
                loop_idx = 0;
            }
            else
            {
                ++loop_idx;
            }
        }
    }

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
