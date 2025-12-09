// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

class SimplePipeline : public simdb::App
{
public:
    static constexpr auto NAME = "pipeline-v2";

    SimplePipeline(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~SimplePipeline() noexcept = default;

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("CompressedData");
        tbl.addColumn("DataBlob", dt::blob_t);
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME);

        pipeline->addStage<CompressionStage>("compressor");
        pipeline->addStage<DatabaseStage>("db_writer");
        pipeline->noMoreStages();

        pipeline->bind("compressor.compressed_data", "db_writer.data_to_write");
        pipeline->noMoreBindings();

        pipeline_head_ = pipeline->getInPortQueue<std::vector<double>>("compressor.input_data");
        pipeline_mgr->finalize(pipeline);
    }

    void process(const std::vector<double>& data)
    {
        pipeline_head_->push(data);
    }

private:
    /// Compress on pipeline thread 1
    class CompressionStage : public simdb::pipeline::Stage
    {
    public:
        CompressionStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo)
            : Stage(name, queue_repo)
        {
            addInPort_<std::vector<double>>("input_data", input_queue_);
            addOutPort_<std::vector<char>>("compressed_data", output_queue_);
        }

    private:
        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            std::vector<double> data;
            if (input_queue_->try_pop(data)) {
                std::vector<char> compressed_data;
                simdb::compressData(data, compressed_data);
                output_queue_->emplace(std::move(compressed_data));
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }

            return simdb::pipeline::RunnableOutcome::NO_OP;
        }

        simdb::ConcurrentQueue<std::vector<double>>* input_queue_ = nullptr;
        simdb::ConcurrentQueue<std::vector<char>>* output_queue_ = nullptr;
    };

    /// Write to SQLite on dedicated database thread
    class DatabaseStage : public simdb::pipeline::DatabaseStage<SimplePipeline>
    {
    public:
        DatabaseStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo)
            : simdb::pipeline::DatabaseStage<SimplePipeline>(name, queue_repo)
        {
            addInPort_<std::vector<char>>("data_to_write", input_queue_);
        }

    private:
        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            std::vector<char> data;
            if (input_queue_->try_pop(data)) {
                auto inserter = getTableInserter_("CompressedData");
                inserter->setColumnValue(0, data);
                inserter->createRecord();
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }

            return simdb::pipeline::RunnableOutcome::NO_OP;
        }

        simdb::ConcurrentQueue<std::vector<char>>* input_queue_ = nullptr;
    };

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(SimplePipeline);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(SimplePipeline::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<SimplePipeline>();
    constexpr int NUM_VECS = 100;
    constexpr int VEC_SIZE = 1000;
    std::vector<std::vector<double>> test_data;

    for (int i = 0; i < NUM_VECS; ++i) {
        std::vector<double> data(VEC_SIZE, static_cast<double>(i));
        test_data.push_back(data);
        app->process(data);
    }

    // Finalize...
    app_mgr.postSimLoopTeardown();

    // Validate...
    auto query = db_mgr.createQuery("CompressedData");

    std::vector<char> blob;
    query->select("DataBlob", blob);

    auto results = query->getResultSet();
    size_t row_idx = 0;
    while (results.getNextRecord())
    {
        std::vector<double> decompressed_data;
        simdb::decompressData(blob, decompressed_data);
        EXPECT_EQUAL(decompressed_data, test_data[row_idx]);
        row_idx++;
    }

    return 0;
}
