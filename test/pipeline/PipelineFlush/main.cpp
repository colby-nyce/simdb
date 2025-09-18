// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/CircularBuffer.hpp"
#include "simdb/pipeline/elements/AsyncDbWriter.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConditionalLock.hpp"
#include "SimDBTester.hpp"

/// This test verifies the behavior of all SimDB-provided pipeline elements.
/// Specifically, it verifies that all elements properly flush their data
/// using the RunnableFlusher class.

class PipelineFlushApp : public simdb::App
{
public:
    static constexpr auto NAME = "pipeline-flush";

    PipelineFlushApp(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~PipelineFlushApp() noexcept = default;

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("TestData");
        tbl.addColumn("IntBlob", dt::blob_t);
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
        simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // 1. CircularBuffer of size 17
        auto circbuf = simdb::pipeline::createTask<simdb::CircularBuffer<int, 17>>();

        // 2. Function that zeros out all integer values divisible by 7
        auto zero7 = simdb::pipeline::createTask<simdb::pipeline::Function<int, int>>(
            [](int&& val, simdb::ConcurrentQueue<int>& out, bool /*force_flush*/)
            {
                if (val % 7 == 0)
                {
                    val = 0;
                }
                out.push(val);
            });

        // 3. Buffer of size 23
        constexpr bool flush_partial = true;
        auto buffer = simdb::pipeline::createTask<simdb::pipeline::Buffer<int>>(23, flush_partial);

        // 4. Function that doubles all integer values that pass through
        using BufferedInts = std::vector<int>;
        auto doubler = simdb::pipeline::createTask<simdb::pipeline::Function<BufferedInts, BufferedInts>>(
            [](BufferedInts&& vals, simdb::ConcurrentQueue<BufferedInts>& out, bool /*force_flush*/)
            {
                for (auto& val : vals)
                {
                    val *= 2;
                }
                out.emplace(std::move(vals));
            });

        // 5. DB writer that writes to TestData table
        auto db_writer = db_accessor->createAsyncWriter<PipelineFlushApp, BufferedInts, int>(
            [max_int = int(-1)]
            (BufferedInts&& vals,
             simdb::ConcurrentQueue<int>& out,
             simdb::pipeline::AppPreparedINSERTs* tables,
             bool /*force_flush*/) mutable
            {
                auto inserter = tables->getPreparedINSERT("TestData");
                inserter->setColumnValue(0, vals);
                inserter->createRecord();

                for (const auto val : vals)
                {
                    max_int = std::max(max_int, val);
                }
                out.push(max_int);
            });

        // 6. Terminating function which sends the max value seen so far back to this app
        auto record_max = simdb::pipeline::createTask<simdb::pipeline::Function<int, void>>(
            [this](int&& val, bool force_flush) mutable
            {
                simdb::ConditionalLock<std::mutex> lock(mutex_, force_flush);
                max_val_ = std::max(max_val_, val);
            });

        // Connect the pipeline elements.
        *circbuf >> *zero7 >> *buffer >> *doubler >> *db_writer >> *record_max;

        // Create the task flusher.
        flusher_ = std::make_unique<simdb::pipeline::RunnableFlusher>(
            *db_mgr_, circbuf, zero7, buffer, doubler, db_writer, record_max);

        // Store the pipeline head.
        pipeline_head_ = circbuf->getTypedInputQueue<int>();

        // Assign thread tasks.
        pipeline->createTaskGroup("PreDB_Thread1")
            ->addTask(std::move(circbuf))
            ->addTask(std::move(zero7))
            ->addTask(std::move(buffer))
            ->addTask(std::move(doubler))
            ->addTask(std::move(record_max));

        return pipeline;
    }

    void process(const int value)
    {
        pipeline_head_->push(value);
    }

    void flushAndValidate(const int last_val)
    {
        // Flush and immediately lock the database to prevent accidentally "cheating".
        // It will take a quick second to recreate the expected final values, and we
        // do not want the pipeline threads to sneak in more data.
        flusher_->flush();

        db_mgr_->safeTransaction([&]()
            {
                // Original values were 1 through last_val.
                std::vector<int> expected_vals;
                expected_vals.reserve(last_val);
                for (int val = 1; val <= last_val; ++val)
                {
                    expected_vals.push_back(val);
                }

                // Zero out all values divisible by 7.
                for (auto& val : expected_vals)
                {
                    if (val % 7 == 0)
                    {
                        val = 0;
                    }
                }

                // Double all values.
                for (auto& val : expected_vals)
                {
                    val *= 2;
                }

                // Find the max value and ensure it equals max_val_.
                int expected_max = -1;
                for (const auto val : expected_vals)
                {
                    expected_max = std::max(expected_max, val);
                }
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    EXPECT_EQUAL(expected_max, max_val_);
                }

                // Now query the database to ensure all values were written as expected.
                std::vector<int> actual_vals;
                auto query = db_mgr_->createQuery("TestData");

                std::vector<int> partial_vals;
                query->select("IntBlob", partial_vals);

                auto results = query->getResultSet();
                while (results.getNextRecord())
                {
                    actual_vals.insert(actual_vals.end(), partial_vals.begin(), partial_vals.end());
                }
                EXPECT_EQUAL(expected_vals, actual_vals);
            });
    }

private:
    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::unique_ptr<simdb::pipeline::RunnableFlusher> flusher_;
    simdb::ConcurrentQueue<int>* pipeline_head_ = nullptr;
    int max_val_ = -1;
    std::mutex mutex_;
};

REGISTER_SIMDB_APPLICATION(PipelineFlushApp);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(PipelineFlushApp::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<PipelineFlushApp>();
    constexpr int STEPS = 1000;

    for (int i = 1; i <= STEPS; ++i)
    {
        app->process(i);
        if (i % 100 == 0)
        {
            app->flushAndValidate(i);
        }
    }

    // Finish...
    app_mgr.postSimLoopTeardown(true);

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
