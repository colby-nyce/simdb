// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

/// This example demonstrates the use of concurrently-running SimDB apps.
/// We will create two instances of the same app writing to the same database
/// from the same thread, and another app writing to a second database on its
/// own DB thread.

class SimplePipeline : public simdb::App
{
public:
    static constexpr auto NAME = "simple-pipeline";

    SimplePipeline(simdb::DatabaseManager* db_mgr, bool share_zlib_threads)
        : db_mgr_(db_mgr)
        , share_zlib_threads_(share_zlib_threads)
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
        auto pipeline = pipeline_mgr->createPipeline(NAME);

        pipeline->addStage<CompressionStage>("compressor", share_zlib_threads_);
        pipeline->addStage<DatabaseStage>("db_writer", getInstance());
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
        CompressionStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, bool share_zlib_threads)
            : Stage(name, queue_repo)
            , share_zlib_threads_(share_zlib_threads)
        {
            addInPort_<std::vector<double>>("input_data", input_queue_);
            addOutPort_<std::vector<char>>("compressed_data", output_queue_);
        }

    private:
        bool shareThreads_() const override
        {
            return share_zlib_threads_;
        }

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

        const bool share_zlib_threads_;
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
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }

            return simdb::pipeline::RunnableOutcome::NO_OP;
        }

        size_t instance_num_ = 0;
        simdb::ConcurrentQueue<std::vector<char>>* input_queue_ = nullptr;
    };

    simdb::DatabaseManager* db_mgr_ = nullptr;
    bool share_zlib_threads_ = false;
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;
};

namespace simdb
{
    /// AppFactory specialization for SimplePipeline. This is needed so we
    /// can pass custom constructor arguments (share_zlib_threads). The
    /// default factory assumes a constructor with only DatabaseManager*.
    template <> class AppFactory<SimplePipeline> : public AppFactoryBase
    {
    public:
        using AppT = SimplePipeline;

        void shareThreads(size_t instance_num, bool share_threads)
        {
            share_threads_by_inst_num_[instance_num] = share_threads;
        }

        AppT* createApp(DatabaseManager* db_mgr, size_t instance_num = 0) override
        {
            bool share_threads = share_threads_by_inst_num_[instance_num];
            return new SimplePipeline(db_mgr, share_threads);
        }

        void defineSchema(Schema& schema) const override
        {
            AppT::defineSchema(schema);
        }

    private:
        std::map<size_t, bool> share_threads_by_inst_num_;
    };
}

REGISTER_SIMDB_APPLICATION(SimplePipeline);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    // Create 4 instances
    app_mgr.enableApp(SimplePipeline::NAME, 4);

    auto factory = app_mgr.getAppFactory<SimplePipeline>();
    factory->shareThreads(1, true);
    factory->shareThreads(2, true);
    factory->shareThreads(3, false);
    factory->shareThreads(4, false);
    app_mgr.createEnabledApps();

    // Should not be able to get app with unspecified instance
    EXPECT_THROW(app_mgr.getApp<SimplePipeline>());

    auto app1 = app_mgr.getApp<SimplePipeline>(1);
    auto app2 = app_mgr.getApp<SimplePipeline>(2);
    auto app3 = app_mgr.getApp<SimplePipeline>(3);
    auto app4 = app_mgr.getApp<SimplePipeline>(4);
    SimplePipeline* apps[] = {app1, app2, app3, app4};

    app_mgr.createSchemas();
    app_mgr.openPipelines();

    // Simulate...
    std::map<size_t, std::vector<std::vector<double>>> test_data;
    for (size_t i = 0; i < 1000; ++i)
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

    return 0;
}
