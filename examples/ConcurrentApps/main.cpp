// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

/// This example demonstrates the use of concurrently-running SimDB apps.
/// We will create four instances of the SimplePipeline app and one instance
/// of the RecordReporter app. All five apps use the same database.

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
                return simdb::pipeline::PROCEED;
            }

            return simdb::pipeline::SLEEP;
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
                return simdb::pipeline::PROCEED;
            }

            return simdb::pipeline::SLEEP;
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

/// This app runs DB queries and prints to stdout notifications
/// about what the SimplePipeline apps have written to the database.
class RecordReporter : public simdb::App
{
public:
    static constexpr auto NAME = "record-reporter";

    RecordReporter(simdb::DatabaseManager*)
    {}

    ~RecordReporter() noexcept = default;

    static void defineSchema(simdb::Schema&)
    {
        // No schema for this app
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME);
        pipeline->addStage<ReporterStage>("reporter");
        pipeline->noMoreStages();
        pipeline->noMoreBindings();
        pipeline_mgr->finalize(pipeline);
    }

private:
    class ReporterStage : public simdb::pipeline::DatabaseStage<RecordReporter>
    {
    public:
        ReporterStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo)
            : DatabaseStage<RecordReporter>(name, queue_repo)
            , tic_(std::chrono::steady_clock::now())
        {
            // No inputs, no outputs
        }

    private:
        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            // Only report twice a second
            auto toc = std::chrono::steady_clock::now();
            auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(toc - tic_);
            if (dur.count() < 500)
            {
                return simdb::pipeline::SLEEP;
            }
            tic_ = std::chrono::steady_clock::now();

            auto db_mgr = getDatabaseManager_();
            auto query = db_mgr->createQuery("CompressedData");

            // SELECT AppInstance, COUNT(*) FROM CompressedData GROUP BY AppInstance ORDER BY AppInstance ASC
            int app_instance;
            query->select("AppInstance", app_instance);

            int record_count;
            query->select("COUNT(*)", record_count);

            query->groupBy("AppInstance");
            query->orderBy("AppInstance", simdb::QueryOrder::ASC);

            bool print_header = true;
            auto results = query->getResultSet();
            while (results.getNextRecord())
            {
                if (print_header)
                {
                    std::cout << "SimplePipeline results written to the database so far:\n";
                    print_header = false;
                }
                std::cout << "\tInstance " << app_instance << " has written " << record_count << " records\n";
            }

            // Even though we technically "did something" here, don't keep the
            // DB thread polling for more work. The return value from the DB
            // stage in the SimplePipeline will control whether the DB thread
            // briefly goes to sleep or not (SLEEP / PROCEED).
            //
            // More importantly than runtime performance, if we always returned
            // PROCEED, then the DB thread would not complete the BEGIN/COMMIT
            // TRANSACTION block that we are implicitly inside right now, and
            // the DB commits will just continue to pile up in the SQLite cache.
            return simdb::pipeline::SLEEP;
        }

        std::chrono::time_point<std::chrono::steady_clock> tic_;
    };
};

REGISTER_SIMDB_APPLICATION(SimplePipeline);
REGISTER_SIMDB_APPLICATION(RecordReporter);

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

    auto factory = app_mgr.getAppFactory<SimplePipeline>();
    factory->shareThreads(1, true);
    factory->shareThreads(2, true);
    factory->shareThreads(3, false);
    factory->shareThreads(4, false);

    // Create 1 instance of the RecordReporter app
    app_mgr.enableApp(RecordReporter::NAME);

    // Create all 5 apps
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
    for (size_t i = 0; i < 50000; ++i)
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

    REPORT_ERROR;
    return ERROR_CODE;
}
