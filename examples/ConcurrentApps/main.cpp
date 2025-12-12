// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

/// This example demonstrates the use of concurrently-running SimDB apps.
/// We will create four instances of the SimplePipeline app below.

class SimplePipeline : public simdb::App
{
public:
    static constexpr auto NAME = "simple-pipeline";

    SimplePipeline(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~SimplePipeline() noexcept = default;

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("CompressedData");
        tbl.addColumn("AppInstance", dt::int32_t);
        tbl.addColumn("DataBlob", dt::blob_t);
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);

        pipeline->addStage<CompressionStage>("compressor");
        pipeline->addStage<DatabaseStage>("db_writer", getInstance());
        pipeline->noMoreStages();

        pipeline->bind("compressor.compressed_data", "db_writer.data_to_write");
        pipeline->noMoreBindings();

        pipeline_head_ = pipeline->getInPortQueue<std::vector<double>>("compressor.input_data");
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
                return simdb::pipeline::PROCEED;
            }

            return simdb::pipeline::SLEEP;
        }

        simdb::ConcurrentQueue<std::vector<double>>* input_queue_ = nullptr;
        simdb::ConcurrentQueue<std::vector<char>>* output_queue_ = nullptr;
    };

    /// Write to SQLite on dedicated database thread
    class DatabaseStage : public simdb::pipeline::DatabaseStage<SimplePipeline>
    {
    public:
        DatabaseStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, size_t instance_num)
            : simdb::pipeline::DatabaseStage<SimplePipeline>(name, queue_repo)
            , instance_num_(instance_num)
        {
            addInPort_<std::vector<char>>("data_to_write", input_queue_);
        }

    private:
        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            std::vector<char> data;
            if (input_queue_->try_pop(data)) {
                auto inserter = getTableInserter_("CompressedData");
                inserter->setColumnValue(0, (int)instance_num_);
                inserter->setColumnValue(1, data);
                inserter->createRecord();
                return simdb::pipeline::PROCEED;
            }

            return simdb::pipeline::SLEEP;
        }

        size_t instance_num_ = 0;
        simdb::ConcurrentQueue<std::vector<char>>* input_queue_ = nullptr;
    };

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(SimplePipeline);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    // Disable pipeline messages; it clutters up stdout with so many running apps
    app_mgr.disableMessageLog();
    app_mgr.disableErrorLog();

    // Create 4 instances of the SimplePipeline app
    app_mgr.enableApp(SimplePipeline::NAME, 4);

    // Create all the apps
    app_mgr.createEnabledApps();

    // Should not be able to get app with unspecified instance
    EXPECT_THROW(app_mgr.getApp<SimplePipeline>());

    auto app1 = app_mgr.getApp<SimplePipeline>(1);
    auto app2 = app_mgr.getApp<SimplePipeline>(2);
    auto app3 = app_mgr.getApp<SimplePipeline>(3);
    auto app4 = app_mgr.getApp<SimplePipeline>(4);
    SimplePipeline* apps[] = {app1, app2, app3, app4};

    // Instantiate the DB schema
    app_mgr.createSchemas();

    // Initialize all pipelines
    app_mgr.initializePipelines();

    // Finally, launch all threads for all app pipelines
    app_mgr.openPipelines();

    // Simulate...
    std::map<size_t, std::vector<std::vector<double>>> test_data;
    for (size_t i = 0; i < 10000; ++i)
    {
        auto data = std::vector<double>(1000);
        for (auto& val : data)
        {
            val = rand() % 100 * M_PI;
        }

        auto instance = rand() % 4 + 1;
        test_data[instance].push_back(data);
        apps[instance - 1]->process(data);
    }

    // Let the threads keep running for a second just to get
    // extra testing for all the running apps. This is really
    // just a sanity check of sorts.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Finish...
    app_mgr.postSimLoopTeardown();

    // Validate...
    for (size_t instance = 1; instance <= 4; ++instance)
    {
        auto query = db_mgr.createQuery("CompressedData");
        query->addConstraintForInt("AppInstance", simdb::Constraints::EQUAL, (int)instance);

        std::vector<char> blob;
        query->select("DataBlob", blob);

        const auto& expected_data = test_data[instance];
        size_t row_idx = 0;

        auto results = query->getResultSet();
        while (results.getNextRecord())
        {
            std::vector<double> actual_data;
            simdb::decompressData(blob, actual_data);
            EXPECT_EQUAL(actual_data, expected_data[row_idx]);
            ++row_idx;
        }
    }

    REPORT_ERROR;
    return ERROR_CODE;
}
