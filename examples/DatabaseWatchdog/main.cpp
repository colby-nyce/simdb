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
        auto pipeline = pipeline_mgr->createPipeline(NAME);

        pipeline->addStage<CompressionStage>("compressor");
        pipeline->addStage<DatabaseStage>("db_writer");
        pipeline->noMoreStages();

        pipeline->bind("compressor.compressed_data", "db_writer.data_to_write");
        pipeline->noMoreBindings();

        pipeline_head_ = pipeline->getInPortQueue<std::vector<double>>("compressor.input_data");
        pipeline_mgr->finalize(pipeline);

        // Store the DB accessor for async queries from the main thread
        async_db_accessor_ = pipeline->getAsyncDatabaseAccessor();

        // Store a pipeline flusher (flush compressor first, then DB writer)
        pipeline_flusher_ = pipeline->createFlusher({"compressor", "db_writer"});

        // Store the pipeline manager so we can test ScopedRunnableDisabler
        pipeline_mgr_ = pipeline_mgr;
    }

    void process(const std::vector<double>& data)
    {
        pipeline_head_->push(data);
    }

    bool done()
    {
        return done_();
    }

    size_t flush()
    {
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

    /// Periodically check for 10k records. We will stop sending new
    /// data when that limit is reached or exceeded.
    bool done_()
    {
        if (finished_)
        {
            return true;
        }

        // Query the DB every 2 seconds
        auto toc = std::chrono::steady_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(toc - tic_);
        if (dur.count() < 2000)
        {
            return false;
        }
        tic_ = std::chrono::steady_clock::now();

        // Query to run asynchronously
        auto async_query = [&](simdb::DatabaseManager* db_mgr)
        {
            auto query = db_mgr->createQuery("CompressedData");
            if (query->count() > TARGET_NUM_RECORDS)
            {
                finished_ = true;
            }
        };

        // Post the query on the database thread with a 5-second timeout
        async_db_accessor_->eval(async_query, 5);

        // Test the use of ScopedRunnableDisabler which will disable all
        // runnables/threads in the pipeline.
        if (finished_)
        {
            // Disable everything while this object is in scope.
            auto disabler = pipeline_mgr_->scopedDisableAll();

            auto query = db_mgr_->createQuery("CompressedData");
            auto num_records = query->count();

            std::cout << "As soon as we hit the target record count of 10k records, ";
            std::cout << "the database had " << num_records << " in it. More may be ";
            std::cout << "coming when we flush the whole pipeline..." << std::endl;
        }

        return finished_;
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;
    simdb::pipeline::AsyncDatabaseAccessor* async_db_accessor_ = nullptr;
    simdb::pipeline::PipelineManager* pipeline_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::Flusher> pipeline_flusher_;
    std::chrono::time_point<std::chrono::steady_clock> tic_;
    bool finished_ = false;
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
        auto pipeline = pipeline_mgr->createPipeline(NAME);
        pipeline->addStage<Watchdog>("watchdog", this);
        pipeline->noMoreStages();
        pipeline->noMoreBindings();
        pipeline_mgr->finalize(pipeline);
    }

    bool done()
    {
        return finished_;
    }

private:
    class Watchdog : public simdb::pipeline::Stage
    {
    public:
        Watchdog(const std::string& name, simdb::pipeline::QueueRepo& queue_repo, DatabaseWatchdog* app)
            : Stage(name, queue_repo)
            , app_(app)
        {
            // No inputs, no outputs
        }

    private:
        bool shareThreads_() const override
        {
            return false;
        }

        simdb::pipeline::RunnableOutcome run_(bool) override
        {
            if (finished_)
            {
                return simdb::pipeline::SLEEP;
            }

            // Query the DB every 3 seconds
            auto toc = std::chrono::steady_clock::now();
            auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(toc - tic_);
            if (dur.count() < 3000)
            {
                return simdb::pipeline::SLEEP;
            }
            tic_ = std::chrono::steady_clock::now();

            // Query to run asynchronously
            size_t num_records = 0;
            auto async_query = [&](simdb::DatabaseManager* db_mgr)
            {
                auto query = db_mgr->createQuery("CompressedData");
                auto num_records = query->count();
                if (num_records > TARGET_NUM_RECORDS)
                {
                    finished_ = true;
                    app_->thresholdReached_();
                }
            };

            // Post the query on the database thread with a 5-second timeout
            auto async_db_accessor = getAsyncDatabaseAccessor_();
            async_db_accessor->eval(async_query, 5);

            // Write total number of records created thus far
            std::cout << "Created " << num_records << " so far...\n";

            // Return SLEEP so this thread goes back to sleep
            return simdb::pipeline::SLEEP;
        }

        DatabaseWatchdog* app_ = nullptr;
        std::chrono::time_point<std::chrono::steady_clock> tic_;
        bool finished_ = false;
    };

    void thresholdReached_()
    {
        finished_ = true;
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    bool finished_ = false;
    friend class Watchdog;
};

REGISTER_SIMDB_APPLICATION(SimplePipeline);
REGISTER_SIMDB_APPLICATION(DatabaseWatchdog);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    //TODO cnyce
    //app_mgr.disableMessageLog();
    //app_mgr.disableErrorLog();

    app_mgr.enableApp(SimplePipeline::NAME);
    app_mgr.enableApp(DatabaseWatchdog::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.openPipelines();

    // Simulate...
    auto pipe = app_mgr.getApp<SimplePipeline>();
    auto watchdog = app_mgr.getApp<DatabaseWatchdog>();
    while (true)
    {
        std::vector<double> data(1000);
        for (auto& val : data)
        {
            val = rand() % 100 * M_PI;
        }

        pipe->process(data);

        if (pipe->done() && watchdog->done())
        {
            // 10k threshold reached
            break;
        }
    }

    // With the threads still running, flush any leftover pipeline
    // data and print out the final number of records (typically
    // over the 10k threshold).
    auto final_record_count = pipe->flush();

    // Finish...
    app_mgr.postSimLoopTeardown();

    // Validate...
    auto query = db_mgr.createQuery("CompressedData");
    EXPECT_EQUAL(query->count(), final_record_count);

    REPORT_ERROR;
    return ERROR_CODE;
}
