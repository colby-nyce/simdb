// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

/// This example demonstrates how to post database queries from
/// the main thread, or from any non-database stage. These queries
/// block execution until the query is picked up and run on the
/// database thread.
///
/// The example also shows how to create a pipeline flusher with
/// an API that will flush each stage in a specific order. Flushes
/// occur in a round-robin fashion until all stages are flushed.
///
/// We will create two apps that run at the same time. The first
/// app will write blobs of compressed data to the database, and
/// the app will periodically query the database from the main
/// thread until at least 10k records have been created. When
/// it sees that 10k have been reached, it will stop sending
/// new data down the pipeline.
///
/// The second app will run a single non-database thread (not
/// to be shared with the non-DB threads from the other app).
/// It will periodically print out the total number of records
/// created thus far, and will stop doing any work when it too
/// sees that at least 10k records have been created.

constexpr size_t TARGET_NUM_RECORDS = 10000;

class SimplePipeline : public simdb::App
{
public:
    static constexpr auto NAME = "simple-pipeline";

    SimplePipeline(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
        , tic_(std::chrono::steady_clock::now())
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
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);

        pipeline->addStage<CompressionStage>("compressor");
        pipeline->addStage<DatabaseStage>("db_writer");
        pipeline->noMoreStages();

        pipeline->bind("compressor.compressed_data", "db_writer.data_to_write");
        pipeline->noMoreBindings();

        // As soon as we call noMoreBindings(), all input/output queues are available
        pipeline_head_ = pipeline->getInPortQueue<std::vector<double>>("compressor.input_data");

        // Store a pipeline flusher (flush compressor first, then DB writer)
        pipeline_flusher_ = pipeline->createFlusher({"compressor", "db_writer"});

        // Store the pipeline manager so we can test ScopedRunnableDisabler as
        // we as access the AsyncDatabaseAccessor later.
        pipeline_mgr_ = pipeline_mgr;
    }

    void process(const std::vector<double>& data)
    {
        pipeline_head_->push(data);
    }

    bool done()
    {
        return finished_;
    }

    size_t flush()
    {
        auto disabler = pipeline_mgr_->scopedDisableAll();
        pipeline_flusher_->flush();

        size_t record_count = 0;
        db_mgr_->safeTransaction([&]()
        {
            auto query = db_mgr_->createQuery("CompressedData");
            record_count = query->count();
        });

        std::cout << "After flushing the pipeline, there were ";
        std::cout << record_count << " records in the database.";
        std::cout << std::endl;

        return record_count;
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

            // Ensure that the AsyncDatabaseAccessor is null - no threads have been created yet
            EXPECT_EQUAL(getAsyncDatabaseAccessor_(), nullptr);
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
        DatabaseStage(const std::string& name, simdb::pipeline::QueueRepo& queue_repo)
            : simdb::pipeline::DatabaseStage<SimplePipeline>(name, queue_repo)
        {
            addInPort_<std::vector<char>>("data_to_write", input_queue_);
        }

    private:
        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            // Ensure we cannot get the AsyncDatabaseAccessor - we are already
            // going to be on the database thread, and should just use the
            // DatabaseManager directly (getDatabaseManager_).
            EXPECT_THROW(getAsyncDatabaseAccessor_());

            std::vector<char> data;
            if (input_queue_->try_pop(data)) {
                auto inserter = getTableInserter_("CompressedData");
                inserter->setColumnValue(0, data);
                inserter->createRecord();
                return simdb::pipeline::PROCEED;
            }

            return simdb::pipeline::SLEEP;
        }

        simdb::ConcurrentQueue<std::vector<char>>* input_queue_ = nullptr;
    };

    void thresholdReached_()
    {
        finished_ = true;
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;
    simdb::pipeline::PipelineManager* pipeline_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::Flusher> pipeline_flusher_;
    std::chrono::time_point<std::chrono::steady_clock> tic_;
    bool finished_ = false;
    friend class DatabaseWatchdog;
};

/// Watchdog app
class DatabaseWatchdog : public simdb::App
{
public:
    static constexpr auto NAME = "db-watchdog";

    DatabaseWatchdog(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~DatabaseWatchdog() noexcept = default;

    static void defineSchema(simdb::Schema&)
    {
        // No schema for this app
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME, this);
        pipeline->addStage<Watchdog>("watchdog", this);
        pipeline->noMoreStages();
        pipeline->noMoreBindings();
    }

    void watch(SimplePipeline* pipeline)
    {
        pipeline_app_ = pipeline;
    }

private:
    class Watchdog : public simdb::pipeline::Stage
    {
    public:
        Watchdog(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, DatabaseWatchdog* app)
            : Stage(name, queue_repo, 500) /* Run async query every 500ms */
            , watchdog_app_(app)
            , tic_(std::chrono::steady_clock::now())
        {
            // No inputs, no outputs
        }

    private:
        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            if (finished_)
            {
                return simdb::pipeline::SLEEP;
            }

            // Query the DB once a second
            auto toc = std::chrono::steady_clock::now();
            auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(toc - tic_);
            if (dur < std::chrono::milliseconds(1000))
            {
                return simdb::pipeline::SLEEP;
            }
            tic_ = std::chrono::steady_clock::now();

            // Query to run asynchronously
            size_t num_records = 0;
            auto async_query = [&](simdb::DatabaseManager* db_mgr)
            {
                auto query = db_mgr->createQuery("CompressedData");
                num_records = query->count();
                if (num_records > TARGET_NUM_RECORDS)
                {
                    finished_ = true;
                }
            };

            // Post the query on the database thread with a 5-second timeout
            auto async_db_accessor = getAsyncDatabaseAccessor_();
            async_db_accessor->eval(async_query, 5);

            // Write total number of records created thus far
            if (num_records > 0)
            {
                std::cout << "Created " << num_records << " so far...\n";
            }

            if (finished_)
            {
                std::cout << "As soon as we hit the target record count of 10k records, ";
                std::cout << "the database had " << num_records << " records in it. More may ";
                std::cout << "be coming when we flush the whole pipeline..." << std::endl;
                watchdog_app_->thresholdReached_();
            }

            // Return SLEEP so this thread goes back to sleep
            return simdb::pipeline::SLEEP;
        }

        DatabaseWatchdog* watchdog_app_ = nullptr;
        std::chrono::time_point<std::chrono::steady_clock> tic_;
        bool finished_ = false;
    };

    void thresholdReached_()
    {
        pipeline_app_->thresholdReached_();
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    SimplePipeline* pipeline_app_ = nullptr;
    friend class Watchdog;
};

REGISTER_SIMDB_APPLICATION(SimplePipeline);
REGISTER_SIMDB_APPLICATION(DatabaseWatchdog);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    app_mgr.disableMessageLog();
    app_mgr.disableErrorLog();

    app_mgr.enableApp(SimplePipeline::NAME);
    app_mgr.enableApp(DatabaseWatchdog::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.initializePipelines();
    app_mgr.openPipelines();

    // Simulate...
    auto pipe = app_mgr.getApp<SimplePipeline>();
    auto watchdog = app_mgr.getApp<DatabaseWatchdog>();
    watchdog->watch(pipe);

    size_t num_sent = 0;
    while (true)
    {
        std::vector<double> data(1000);
        for (auto& val : data)
        {
            val = rand() % 100 * M_PI;
        }

        pipe->process(data);
        ++num_sent;

        if (pipe->done())
        {
            // 10k threshold reached
            break;
        }

        // Throttle the data so the test doesn't run too long.
        // Specifically, the flush() call below can take a while
        // since we can send so much data (well over the 10k
        // we are using the watchdog for) by the time the watchdog
        // sees we have 10k and can stop.
        if (num_sent % 1000 == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Flush any leftover pipeline and print out the final number
    // of records in the database.
    auto num_records = pipe->flush();
    EXPECT_EQUAL(num_records, num_sent);

    // Finish...
    app_mgr.postSimLoopTeardown();

    REPORT_ERROR;
    return ERROR_CODE;
}
