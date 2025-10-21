// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/Random.hpp"
#include "simdb/utils/TickTock.hpp"
#include "SimDBTester.hpp"

// This test shows how to build a pipeline with a RunnableFlusher
// whose tasks can be short-circuited in the call to flush(). The
// use case shown here will use a DB sink task that will be aborted
// as soon as a specific piece of data is available that the main
// thread is waiting on.

enum class Cancellability
{
    DISABLED,
    DB_QUERY_CANCEL_ONLY,
    STREAMING_TICKS_CANCEL_ONLY,
    BOTH_CANCELS_ENABLED
};

class CancelledTask : public simdb::App
{
public:
    static constexpr auto NAME = "cancelled-task";

    CancelledTask(simdb::DatabaseManager* db_mgr, Cancellability cancellability)
        : db_mgr_(db_mgr)
        , cancellability_(cancellability)
    {}

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("SimDataBlobs");
        tbl.addColumn("Tick", dt::uint64_t);
        tbl.addColumn("Data", dt::blob_t);
        tbl.createIndexOn("Tick");
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        auto pipeline = pipeline_mgr->createPipeline(NAME);
        auto db_accessor = pipeline_mgr->getAsyncDatabaseAccessor();

        // Task 1: Used in order to short-circuit RunnableFlusher. Will only be enabled
        // prior to a flush and disabled right after.
        auto db_query_flush_abort = simdb::pipeline::createTask<simdb::pipeline::Function<void, SimData>>(
            [this](simdb::ConcurrentQueue<SimData>& out, bool force)
            {
                // Note that the pipeline head (first stop for new SimData) is the second
                // task, not this one. We use Function<void,SimData> simply to allow this
                // task to be connected to the zlib task, which is typically the real start
                // of the pipeline. See pipeline_head_.
                (void)out;

                // This task should never have been enabled unless we are configured to
                // use cancellable flushes.
                if (cancellability_ == Cancellability::DISABLED)
                {
                    throw simdb::DBException("Should not be running this task!");
                }

                // Early return if force=false, which can happen even during a main thread
                // flush. If true, it means we are executing on one of the pipeline threads
                // while a RunnableFlusher is flushing and there is nothing to do.
                if (!force)
                {
                    return simdb::pipeline::RunnableOutcome::NO_OP;
                }

                // If we are enabled while flushing, run a query to see if our pending tick's
                // data has been committed to the database yet, and abort the flush if so.
                std::lock_guard<std::mutex> lock(mutex_);
                if (!pending_request_.pending)
                {
                    throw simdb::DBException("Should not be running this task!");
                }

                // Note that we can safely access the database here, since RunnableFlusher
                // executes inside a safeTransaction. The database thread is implicitly blocked
                // during those flush operations.
                auto query = db_mgr_->createQuery("SimDataBlobs");
                query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, pending_request_.tick);

                if (query->count())
                {
                    // It's in the database! Abort the flush.
                    pending_request_.pending = false;
                    db_query_flush_aborted_ = true;
                    return simdb::pipeline::RunnableOutcome::ABORT_FLUSH;
                }

                // Not there yet. Keep going.
                return simdb::pipeline::RunnableOutcome::NO_OP;
            }
        );

        // Task 2: Introduce a little latency in the pipeline by compressing the data blobs.
        auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<SimData, CompressedSimData>>(
            [](SimData&& input, simdb::ConcurrentQueue<CompressedSimData>& out, bool /*force*/)
            {
                CompressedSimData compressed;
                compressed.tick = input.tick;
                simdb::compressData(input.data, compressed.compressed_data);
                out.emplace(std::move(compressed));

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Task 3: Write the sim data to the database.
        auto db_task = db_accessor->createAsyncWriter<CancelledTask, CompressedSimData, uint64_t>(
            [this](CompressedSimData&& input,
                   simdb::ConcurrentQueue<uint64_t>& out,
                   simdb::pipeline::AppPreparedINSERTs* tables,
                   bool force)
            {
                auto inserter = tables->getPreparedINSERT("SimDataBlobs");
                inserter->setColumnValue(0, input.tick);
                inserter->setColumnValue(1, input.compressed_data);
                inserter->createRecord();

                // Just in case the final task is enabled during a flush, which is true
                // when using RunnableFlusher but not true during normal end-of-sim flush,
                // send this data blob's tick downstream to give the ABORT_FLUSH a chance
                // to run.
                if (force)
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (pending_request_.pending)
                    {
                        out.push(input.tick);
                    }
                }

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Task 4: Used in order to short-circuit RunnableFlusher. Will only be enabled
        // prior to a flush and disabled right after. Unlike the first pipeline task
        // which implements a more robust but more expensive flush abort, this task
        // will just look at the incoming committed ticks without a database query.
        auto streaming_ticks_flush_abort = simdb::pipeline::createTask<simdb::pipeline::Function<uint64_t, void>>(
            [this](uint64_t&& tick, bool force)
            {
                // This task should never have been enabled unless we are configured to
                // use cancellable flushes.
                if (cancellability_ == Cancellability::DISABLED)
                {
                    throw simdb::DBException("Should not be running this task!");
                }

                // Early return if force=false, which can happen even during a main thread
                // flush. If true, it means we are executing on one of the pipeline threads
                // while a RunnableFlusher is flushing and there is nothing to do.
                if (!force)
                {
                    return simdb::pipeline::RunnableOutcome::NO_OP;
                }

                // If we are enabled while flushing, see if our pending tick matches the
                // tick that was just committed to the database, and abort the flush if so.
                std::lock_guard<std::mutex> lock(mutex_);
                if (!pending_request_.pending)
                {
                    throw simdb::DBException("Should not be running this task!");
                }

                if (pending_request_.tick == tick)
                {
                    // The tick we are waiting on was just committed to the database.
                    pending_request_.pending = false;
                    streaming_ticks_flush_aborted_ = true;
                    return simdb::pipeline::RunnableOutcome::ABORT_FLUSH;
                }

                // Not there yet. Keep going.
                return simdb::pipeline::RunnableOutcome::NO_OP;
            }
        );

        // Disable the ABORT_FLUSH tasks now. They will only be enabled when we are
        // about to run a RunnableFlusher (if we are configured for cancellable
        // flushes).
        db_query_flush_abort->enable(false);
        streaming_ticks_flush_abort->enable(false);

        // Save the above cancellation tasks for later so we can toggle them on/off.
        switch (cancellability_)
        {
            case Cancellability::BOTH_CANCELS_ENABLED:
            {
                db_query_flush_abort_ = db_query_flush_abort.get();
                streaming_ticks_flush_abort_ = streaming_ticks_flush_abort.get();
                break;
            }

            case Cancellability::DB_QUERY_CANCEL_ONLY:
            {
                db_query_flush_abort_ = db_query_flush_abort.get();
                break;
            }

            case Cancellability::STREAMING_TICKS_CANCEL_ONLY:
            {
                streaming_ticks_flush_abort_ = streaming_ticks_flush_abort.get();
                break;
            }

            default: break;
        }

        *db_query_flush_abort >> *zlib_task >> *db_task >> *streaming_ticks_flush_abort;

        // Start the normal flow of data into the pipeline at the second task since
        // the first task is only used to abort RunnableFlusher.
        pipeline_head_ = zlib_task->getTypedInputQueue<SimData>();

        pipeline_flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(
            *db_mgr_, db_query_flush_abort, zlib_task, db_task, streaming_ticks_flush_abort);

        pipeline->createTaskGroup("ZlibPipeline")
            ->addTask(std::move(db_query_flush_abort))
            ->addTask(std::move(zlib_task))
            ->addTask(std::move(streaming_ticks_flush_abort));
    }

    void process(uint64_t tick, const std::vector<double>& data)
    {
        SimData sim_data;
        sim_data.tick = tick;
        sim_data.data = data;
        pipeline_head_->emplace(std::move(sim_data));
    }

    std::vector<double> getDataAtTick5000()
    {
        if (cancellability_ != Cancellability::DISABLED)
        {
            // Get ready for the cancellable flush.
            {
                std::lock_guard<std::mutex> lock(mutex_);
                pending_request_.tick = 5000;
                pending_request_.pending = true;
            }

            if (db_query_flush_abort_)
            {
                db_query_flush_abort_->enable(true);
            }

            if (streaming_ticks_flush_abort_)
            {
                streaming_ticks_flush_abort_->enable(true);
            }

            // Perform a "round robin" flush to give the ABORT_FLUSH task(s)
            // many more chances to see the 5000th tick and abort the flush early.
            //
            // This is much faster than a waterfall flush which would have to wait
            // for all earlier tasks (e.g. compress all, write all to DB) to finish
            // processing all their data before the trailing abort task gets to run.
            pipeline_flusher_->roundRobinFlush();

            // Ensure that one of the ABORT_FLUSH tasks found the data.
            {
                std::lock_guard<std::mutex> lock(mutex_);
                EXPECT_FALSE(pending_request_.pending);
            }

            if (db_query_flush_abort_)
            {
                db_query_flush_abort_->enable(false);
            }

            if (streaming_ticks_flush_abort_)
            {
                streaming_ticks_flush_abort_->enable(false);
            }
        }
        else
        {
            // If we are not using cancellable tasks, then just to a waterfall flush. The
            // round robin flush is really only used to speed up cancellable RunnableFlush.
            pipeline_flusher_->waterfallFlush();
        }

        // Return the sim data at tick 5000.
        auto query = db_mgr_->createQuery("SimDataBlobs");
        query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, 5000);

        std::vector<char> compressed_data;
        query->select("Data", compressed_data);

        auto results = query->getResultSet();
        EXPECT_TRUE(results.getNextRecord());

        std::vector<double> data;
        simdb::decompressData(compressed_data, data);
        return data;
    }

    void postTeardown() override
    {
        if (cancellability_ != Cancellability::DISABLED)
        {
            // Test should be written in a way that guarantees we cancelled
            // the RunnableFlusher.
            EXPECT_TRUE(db_query_flush_aborted_ || streaming_ticks_flush_aborted_);
            EXPECT_FALSE(pending_request_.pending);

            if (db_query_flush_aborted_)
            {
                std::cout << "RunnableFlusher was cancelled with the DB query task" << std::endl;
            }

            if (streaming_ticks_flush_aborted_)
            {
                std::cout << "RunnableFlusher was cancelled with the streaming ticks task" << std::endl;
            }
        }
    }

private:
    struct SimData
    {
        uint64_t tick;
        std::vector<double> data;
    };

    struct CompressedSimData
    {
        uint64_t tick;
        std::vector<char> compressed_data;
    };

    struct PendingRequest
    {
        uint64_t tick;
        bool pending = false;
    } pending_request_;

    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::RunnableFlusher> pipeline_flusher_;
    simdb::ConcurrentQueue<SimData>* pipeline_head_ = nullptr;
    simdb::pipeline::Runnable* db_query_flush_abort_ = nullptr;
    simdb::pipeline::Runnable* streaming_ticks_flush_abort_ = nullptr;
    bool db_query_flush_aborted_ = false;
    bool streaming_ticks_flush_aborted_ = false;
    const Cancellability cancellability_;
    mutable std::mutex mutex_;
};

REGISTER_SIMDB_APPLICATION(CancelledTask);

TEST_INIT;

namespace simdb
{

template <>
class AppFactory<CancelledTask> : public AppFactoryBase
{
public:
    using AppT = CancelledTask;

    void setCancellability(Cancellability cancellability)
    {
        cancellability_ = cancellability;
    }

    AppT* createApp(DatabaseManager* db_mgr) override
    {
        return new AppT(db_mgr, cancellability_);
    }

    void defineSchema(Schema& schema) const override
    {
        AppT::defineSchema(schema);
    }

private:
    Cancellability cancellability_ = Cancellability::DISABLED;
};

} // namespace simdb

void RunTest(Cancellability cancellability)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    // Setup...
    app_mgr.enableApp(CancelledTask::NAME);
    app_mgr.getAppFactory<CancelledTask>()->setCancellability(cancellability);
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<CancelledTask>();
    constexpr uint64_t TICKS = 10000;

    // Hold onto a copy of the 5000th tick's data so we can verify it later
    std::vector<double> tick_5000_data;

    for (uint64_t tick = 1; tick <= TICKS; ++tick)
    {
        auto rnd_data = simdb::utils::generateRandomData<double>(1000);

        if (tick == 5000) {
            tick_5000_data = rnd_data;
        }
        app->process(tick, rnd_data);
    }

    // Ask for the 5000th tick's data and time the operation to see
    // the performance difference with cancellation enabled.
    {
        std::ostringstream oss;
        oss << "Retrieve tick 5000 data ";
        switch (cancellability)
        {
            case Cancellability::DISABLED:
            {
                oss << "(no cancellation)";
                break;
            }
            case Cancellability::DB_QUERY_CANCEL_ONLY:
            {
                oss << "(DB query cancellation only)";
                break;
            }
            case Cancellability::STREAMING_TICKS_CANCEL_ONLY:
            {
                oss << "(streaming ticks cancellation only)";
                break;
            }
            case Cancellability::BOTH_CANCELS_ENABLED:
            {
                oss << "(both cancellable tasks enabled)";
                break;
            }
        }

        PROFILE_BLOCK(oss.str().c_str());
        EXPECT_EQUAL(app->getDataAtTick5000(), tick_5000_data);
    }

    // Finish...
    app_mgr.postSimLoopTeardown();
}

int main(int argc, char** argv)
{
    if (argc == 1)
    {
        RunTest(Cancellability::DISABLED);
        RunTest(Cancellability::DB_QUERY_CANCEL_ONLY);
        RunTest(Cancellability::STREAMING_TICKS_CANCEL_ONLY);
        RunTest(Cancellability::BOTH_CANCELS_ENABLED);
    }
    else
    {
        auto idx = std::atoi(argv[1]);
        if (idx < 0 || idx > 3)
        {
            throw simdb::DBException("Invalid test index");
        }
        RunTest(static_cast<Cancellability>(idx));
    }

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
