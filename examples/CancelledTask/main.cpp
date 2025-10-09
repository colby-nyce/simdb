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

class CancelledTask : public simdb::App
{
public:
    static constexpr auto NAME = "cancelled-task";

    CancelledTask(simdb::DatabaseManager* db_mgr, bool cancellable)
        : db_mgr_(db_mgr)
        , cancellable_(cancellable)
    {}

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("SimDataBlobs");
        tbl.addColumn("Tick", dt::uint64_t);
        tbl.addColumn("Data", dt::blob_t);
        tbl.createIndexOn("Tick");
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
        simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

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

        auto db_task = db_accessor->createAsyncWriter<CancelledTask, CompressedSimData, void>(
            [this](CompressedSimData&& input,
                   simdb::pipeline::AppPreparedINSERTs* tables,
                   bool force)
            {
                auto inserter = tables->getPreparedINSERT("SimDataBlobs");
                inserter->setColumnValue(0, input.tick);
                inserter->setColumnValue(1, input.compressed_data);
                inserter->createRecord();

                if (cancellable_ && force)
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (pending_request_.pending && input.tick >= pending_request_.tick)
                    {
                        pending_request_.pending = false;
                        return simdb::pipeline::RunnableOutcome::ABORT_FLUSH;
                    }
                }

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        *zlib_task >> *db_task;

        pipeline_head_ = zlib_task->getTypedInputQueue<SimData>();

        pipeline_flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(
            *db_mgr_, zlib_task, db_task);

        pipeline->createTaskGroup("ZlibPipeline")
            ->addTask(std::move(zlib_task));

        return pipeline;
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
        if (cancellable_)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_request_.tick = 5000;
            pending_request_.pending = true;
        }

        // Perform a "round robin" flush to give the DB task (the last task
        // in the pipeline) many more chances to see the 5000th tick and
        // abort the flush early.
        //
        // This is much faster than a waterfall flush which would have to
        // wait for all earlier tasks (e.g. compression) to finish processing
        // all their data before the DB task even sees the 5000th tick.
        pipeline_flusher_->roundRobinFlush();

        auto query = db_mgr_->createQuery("SimDataBlobs");
        query->addConstraintForUInt64("Tick", simdb::Constraints::EQUAL, 5000);

        std::vector<char> compressed_data;
        query->select("Data", compressed_data);

        auto results = query->getResultSet();
        EXPECT_TRUE(results.getNextRecord());

        if (compressed_data.empty())
        {
            return {};
        }

        std::vector<double> data;
        simdb::decompressData(compressed_data, data);
        return data;
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
    const bool cancellable_;
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

    void setCancellable(bool cancellable)
    {
        cancellable_ = cancellable;
    }

    AppT* createApp(DatabaseManager* db_mgr) override
    {
        return new AppT(db_mgr, cancellable_);
    }

    void defineSchema(Schema& schema) const override
    {
        AppT::defineSchema(schema);
    }

private:
    bool cancellable_ = false;
};

} // namespace simdb

void RunTest(bool cancellable)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    // Setup...
    app_mgr.enableApp(CancelledTask::NAME);
    app_mgr.getAppFactory<CancelledTask>()->setCancellable(cancellable);
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
        const std::string blk = "Retrieve tick 5000 data (" +
            std::string(cancellable ? "cancellable" : "non-cancellable") + ")";
        PROFILE_BLOCK(blk.c_str())

        EXPECT_EQUAL(app->getDataAtTick5000(), tick_5000_data);
    }

    // Finish...
    app_mgr.postSimLoopTeardown();
}

int main()
{
    RunTest(false);
    RunTest(true);

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
