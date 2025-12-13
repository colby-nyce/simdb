// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "SimplePipeline.hpp"

/// This example demonstrates how to post database queries from
/// the main thread, or from any non-database stage. These queries
/// block execution until the query is picked up and run on the
/// database thread.
///
/// The example also shows how to create a pipeline flusher with
/// an API that will flush each stage in a specific order.
///
/// We will create two apps that run at the same time. The first
/// app will write blobs of compressed data to the database with
/// no upper limit to the amount of processed data.
///
/// The second app will run a single non-database thread (not
/// to be shared with the non-DB threads from the other app).
/// It will periodically print out the total number of records
/// created thus far, and will tell the first app (SimplePipeline)
/// to stop accepting new data when it sees that at least 10k
/// records have been created.

constexpr size_t TARGET_NUM_RECORDS = 10000;

class WatchedPipeline : public SimplePipeline
{
public:
    WatchedPipeline(simdb::DatabaseManager* db_mgr)
        : SimplePipeline(db_mgr)
    {}

    ~WatchedPipeline() noexcept = default;

    bool done()
    {
        return finished_;
    }

    size_t flush()
    {
        // Disable the pipeline while the "disabler" is in scope.
        auto disabler = pipeline_mgr_->scopedDisableAll();

        // Flush the compressor stage, the flush the database stage.
        pipeline_flusher_->flush();

        auto query = db_mgr_->createQuery("CompressedData");
        size_t record_count = query->count();

        std::cout << "After flushing the pipeline, there were ";
        std::cout << record_count << " records in the database.";
        std::cout << std::endl;

        return record_count;
    }

private:
    void thresholdReached_()
    {
        finished_ = true;
    }

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

    void watch(WatchedPipeline* pipeline)
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
        {
            // No inputs, no outputs
        }

    private:
        simdb::pipeline::PipelineAction run_(bool) override
        {
            if (finished_)
            {
                return simdb::pipeline::SLEEP;
            }

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

            // Post the query on the database thread with a 5-second timeout.
            // Note that the timeout is used to prevent "runaway" tests in
            // the event of a bug. The only thing we will actually end up
            // waiting on is for the database thread to COMMIT TRANSACTION
            // for whatever it was in the middle of doing, so in reality
            // we only wait for a fraction of a second in typical uses.
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
        bool finished_ = false;
    };

    void thresholdReached_()
    {
        pipeline_app_->thresholdReached_();
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    WatchedPipeline* pipeline_app_ = nullptr;
    friend class Watchdog;
};

REGISTER_SIMDB_APPLICATION(WatchedPipeline);
REGISTER_SIMDB_APPLICATION(DatabaseWatchdog);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    app_mgr.disableMessageLog();
    app_mgr.disableErrorLog();

    app_mgr.enableApp(WatchedPipeline::NAME);
    app_mgr.enableApp(DatabaseWatchdog::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.initializePipelines();
    app_mgr.openPipelines();

    // Simulate...
    auto pipe = app_mgr.getApp<WatchedPipeline>();
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

    // Flush any leftover pipeline data and print out the final number
    // of records in the database.
    auto num_records = pipe->flush();
    EXPECT_EQUAL(num_records, num_sent);

    // Finish...
    app_mgr.postSimLoopTeardown();

    REPORT_ERROR;
    return ERROR_CODE;
}
