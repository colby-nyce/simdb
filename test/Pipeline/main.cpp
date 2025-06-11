#include "simdb/apps/AppRegistration.hpp"
#include "simdb/apps/PipelineApp.hpp"
#include "simdb/test/SimDBTester.hpp"
#include <iostream>
#include <random>

/// This test uses the SimDB pipeline framework to demonstrate how to
/// create a pipeline application that processes data entries through
/// a series of stages. Each stage can be customized to perform specific
/// operations on the data, such as compression, serialization, or
/// transformation.

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

static void UpdateCalledChainLinks(simdb::PipelineEntryBase& entry, const std::string& func_name)
{
    auto db_mgr = entry.getDatabaseManager();
    auto tick = entry.getTick();
    auto compressed = entry.compressed() ? 1 : 0;
    auto db_id = entry.getCommittedDbId();

    auto record = db_mgr->INSERT(
        SQL_TABLE("CalledChainLinks"),
        SQL_COLUMNS("Tick", "FunctionName", "EntryCompressed", "EntryDbId"),
        SQL_VALUES(tick, func_name, compressed, db_id));

    if (!db_id)
    {
        entry.setCommittedDbId(record->getId());
    }
}

static void ProcessCollectorEntry(simdb::PipelineEntryBase& entry)
{
    UpdateCalledChainLinks(entry, "ProcessCollectorEntry");
}

static void ProcessPipelineCollectorEntry(simdb::PipelineEntryBase& entry)
{
    UpdateCalledChainLinks(entry, "ProcessPipelineCollectorEntry");
}

void DoExtraStuff(simdb::PipelineEntryBase& entry)
{
    UpdateCalledChainLinks(entry, "DoExtraStuff");
}

// ------------------------------------------------------------------------
class UniformSerializer : public simdb::PipelineApp
{
public:
    UniformSerializer(simdb::AppPipeline& pipeline, simdb::PipelineChain serialization_chain = simdb::PipelineChain())
        : simdb::PipelineApp(pipeline, serialization_chain + ProcessCollectorEntry)
    {
    }
};

// ------------------------------------------------------------------------
class StatsCollector : public UniformSerializer
{
public:
    StatsCollector(simdb::AppPipeline& pipeline, simdb::PipelineChain serialization_chain = simdb::PipelineChain())
        : UniformSerializer(pipeline, serialization_chain + ProcessPipelineCollectorEntry)
    {
    }
};

// ------------------------------------------------------------------------
int main()
{
    DB_INIT;

    simdb::Schema schema;
    using dt = simdb::SqlDataType;

    auto& funcs_tbl = schema.addTable("CalledChainLinks");
    funcs_tbl.addColumn("Tick", dt::int64_t);
    funcs_tbl.addColumn("FunctionName", dt::string_t);
    funcs_tbl.addColumn("EntryCompressed", dt::int32_t);
    funcs_tbl.addColumn("EntryDbId", dt::int32_t);

    // Make the verification step faster
    funcs_tbl.createCompoundIndexOn({"Tick", "FunctionName"});

    simdb::DatabaseManager db_mgr("test.db");
    db_mgr.appendSchema(schema);

    simdb::AppPipeline pipeline(&db_mgr);
    auto pipeline_collector = std::make_unique<StatsCollector>(pipeline);

    constexpr auto NUM_TICKS = 100;

    for (int tick = 0; tick < NUM_TICKS; ++tick)
    {
        // Since we have a std::vector<double> of data and the PipelineEntryBase
        // only uses std::vector<char>, we can use a utility from the simdb::PipelineApp
        // to convert our vector to a char vector. Converting data through the
        // simdb::PipelineApp has a performance advantage of using a pool of char
        // vectors under the hood to prevent unnecessary allocations.
        simdb::VectorSerializer<double> vector =
            pipeline_collector->createVectorSerializer<double>();

        vector = generateRandomData(1000);

        // Process the data and tell the pipeline to append DoExtraStuff()
        // to the serialization chain. On the database write thread, these
        // functions will get called:
        //
        //   ProcessCollectorEntry()          |-- These came from the simdb::PipelineApp
        //   ProcessPipelineCollectorEntry()  |-- class hierarchy and always process
        //                                    |-- from the base class down.
        //   DoExtraStuff()
        pipeline_collector->process(tick, std::move(vector), DoExtraStuff);
    }

    pipeline.teardown();

    // Validate
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
            "ProcessPipelineCollectorEntry",
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
