#include "simdb/apps/AppPipeline.hpp"
#include "simdb/utils/VectorSerializer.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
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

static void ProcessAppEntry(simdb::PipelineEntryBase& entry)
{
    UpdateCalledChainLinks(entry, "ProcessAppEntry");
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

class App
{
public:
    virtual ~App() = default;
};

// ------------------------------------------------------------------------
class PipelineApp : public App
{
public:
    PipelineApp(simdb::AppPipeline& pipeline, simdb::PipelineChain serialization_chain = simdb::PipelineChain())
        : pipeline_(pipeline)
        , serialization_chain_(serialization_chain + ProcessAppEntry)
        , serialization_stage_(pipeline.getSerializationStage())
    {
        // Now that we are in the base class, reverse the chain to ensure
        // that we run the chain links in the correct order. The base class
        // should be the first link in the chain and on down the class
        // hierarchy. For instance, a base class might handle writing to
        // the database, and subclasses will need the DB ID for their
        // entry processing functions.
        serialization_chain_.reverse();
    }

    void process(uint64_t tick, std::vector<char>&& data, simdb::PipelineFunc on_serialized = nullptr)
    {
        simdb::PipelineEntryBase entry(tick, pipeline_.getDatabaseManager(), std::move(data));
        auto& chain = entry.getStageChain(serialization_stage_);
        chain += serialization_chain_;
        if (on_serialized)
        {
            chain += on_serialized;
        }
        pipeline_.processEntry(std::move(entry));
    }

    template <typename T>
    void process(uint64_t tick, const std::vector<T>& data, simdb::PipelineFunc on_serialized = nullptr)
    {
        simdb::VectorSerializer<T> serializer = createVectorSerializer<T>(&data);
        process(tick, std::move(serializer), on_serialized);
    }

    template <typename T>
    void process(uint64_t tick, simdb::VectorSerializer<T>&& serializer, simdb::PipelineFunc on_serialized = nullptr)
    {
        std::vector<char> data = serializer.release();
        process(tick, std::move(data), on_serialized);
    }

    template <typename T>
    simdb::VectorSerializer<T> createVectorSerializer(const std::vector<T>* initial_data = nullptr)
    {
        std::vector<char> serialized_data;
        reusable_buffers_.try_pop(serialized_data);
        return simdb::VectorSerializer<T>(std::move(serialized_data), initial_data);
    }

private:
    simdb::AppPipeline& pipeline_;
    simdb::PipelineChain serialization_chain_;
    simdb::ConcurrentQueue<std::vector<char>> reusable_buffers_;
    simdb::PipelineStage* serialization_stage_ = nullptr;
};

// ------------------------------------------------------------------------
class UniformSerializer : public PipelineApp
{
public:
    UniformSerializer(simdb::AppPipeline& pipeline, simdb::PipelineChain serialization_chain = simdb::PipelineChain())
        : PipelineApp(pipeline, serialization_chain + ProcessCollectorEntry)
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
        //   ProcessAppEntry()                |-- These came from the PipelineApp
        //   ProcessCollectorEntry()          |-- class hierarchy and always process
        //   ProcessPipelineCollectorEntry()  |-- from the base class down.
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

        // First verify that exactly 4 functions were called
        // in the serialization chain.
        EXPECT_EQUAL(query->count(), 4);

        // Now verify that the functions were called in the
        // correct order.
        const char* expected_funcs[] = {
            "ProcessAppEntry",
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
            if (loop_idx == 3)
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
