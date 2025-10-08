// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/AsyncDbWriter.hpp"
#include "simdb/sqlite/PreparedINSERT.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

static constexpr auto INVALID_TICK = std::numeric_limits<int64_t>::max();

struct TestData
{
    std::vector<uint64_t> data;
    int64_t tick = INVALID_TICK;

    static TestData createRandom(int64_t tick)
    {
        TestData data;
        data.tick = tick;
        for (auto& val : data.data)
        {
            val = rand();
        }
        return data;
    }
};

struct CompressedTestData
{
    std::vector<char> compressed_bytes;
    int64_t tick = INVALID_TICK;

    /// Provided for std::priority_queue (ReorderBuffer)
    bool operator>(const CompressedTestData& other) const
    {
        return tick > other.tick;
    }
};

/// Here is an example of how to create your own pipeline element.
/// We need this at the end of our pipeline to put the compressed
/// data back in order since it is compressed in a thread pool.
class ReorderBuffer {};

namespace simdb::pipeline {

/// This user-defined Task element buffers incoming data and only
/// emits them when it can guarantee they are back in order. We
/// order them by tick starting at 1.
template <>
class Task<ReorderBuffer> : public NonTerminalTask<CompressedTestData, CompressedTestData>
{
private:
    /// Process one item from the queue.
    bool processOne(bool /*force*/) override
    {
        bool did_work = false;
        CompressedTestData data;

        std::lock_guard<std::mutex> lock(mutex_);

        if (this->input_queue_->get().try_pop(data))
        {
            rob_.push(data);
            did_work = true;
        }

        if (!rob_.empty() && rob_.top().tick == next_expected_tick_)
        {
            ++next_expected_tick_;
            this->output_queue_->get().emplace(std::move(rob_.top()));
            rob_.pop();
            did_work = true;
        }
 
        return did_work;
    }

    /// Process all items from the queue.
    bool processAll(bool /*force*/) override
    {
        bool did_work = false;
        CompressedTestData data;

        std::lock_guard<std::mutex> lock(mutex_);

        while (this->input_queue_->get().try_pop(data))
        {
            rob_.push(data);
            did_work = true;
        }

        while (!rob_.empty() && rob_.top().tick == next_expected_tick_)
        {
            ++next_expected_tick_;
            this->output_queue_->get().emplace(std::move(rob_.top()));
            rob_.pop();
            did_work = true;
        }
 
        return did_work;
    }

    std::string getDescription_() const override
    {
        return "ReorderBuffer";
    }

    int64_t next_expected_tick_ = 1;

    std::priority_queue<CompressedTestData,
                        std::vector<CompressedTestData>,
                        std::greater<CompressedTestData>> rob_;

    /// Mutex to protect the ROB
    std::mutex mutex_;
};

} // namespace simdb::pipeline

class CompressionPool : public simdb::App
{
public:
    static constexpr auto NAME = "compression-pool";

    CompressionPool(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~CompressionPool() noexcept = default;

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("CompressedData");
        tbl.addColumn("Tick", dt::int64_t);
        tbl.addColumn("CompressedBytes", dt::blob_t);
        tbl.createIndexOn("Tick");
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(
        simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Create 4 inputs for our pool. These tasks will run on their own threads.
        auto create_compressor = [&]()
        {
            return simdb::pipeline::createTask<simdb::pipeline::Function<TestData, CompressedTestData>>(
                [](TestData&& in,
                   simdb::ConcurrentQueue<CompressedTestData>& out,
                   bool /*force*/)
                {
                    CompressedTestData compressed;
                    compressed.tick = in.tick;
                    simdb::compressData(in.data, compressed.compressed_bytes);
                    out.emplace(std::move(compressed));
                }
            );
        };

        auto compressor1 = create_compressor();
        auto compressor2 = create_compressor();
        auto compressor3 = create_compressor();
        auto compressor4 = create_compressor();

        // Now create the ReorderBuffer to put the compressed packets back in order.
        auto rob = simdb::pipeline::createTask<ReorderBuffer>();

        // The final task will write compressed values to the database. These will
        // implicitly be done in batched BEGIN/COMMIT TRANSACTION blocks along with
        // other database work going on at the moment. The database writes are always
        // executed on the shared database thread.
        auto sqlite = db_accessor->createAsyncWriter<CompressionPool, CompressedTestData, void>(
            [](CompressedTestData&& in,
               simdb::pipeline::AppPreparedINSERTs* tables,
               bool /*force*/)
            {
                auto inserter = tables->getPreparedINSERT("CompressedData");
                inserter->setColumnValue(0, in.tick);
                inserter->setColumnValue(1, in.compressed_bytes);
                inserter->createRecord();
            }
        );

        // Connect the input/output queues together.
        *compressor1 >> *rob;
        *compressor2 >> *rob;
        *compressor3 >> *rob;
        *compressor4 >> *rob;
        *rob >> *sqlite;

        // Get all the input queues. We will randomly choose one every time
        // a data packet arrives, but imagine it was a smarter thread manager
        // with load balancing...
        pool_input_queues_.push_back(compressor1->getTypedInputQueue<TestData>());
        pool_input_queues_.push_back(compressor2->getTypedInputQueue<TestData>());
        pool_input_queues_.push_back(compressor3->getTypedInputQueue<TestData>());
        pool_input_queues_.push_back(compressor4->getTypedInputQueue<TestData>());

        // Ask the pipeline to put each compressor on its own thread.
        pipeline->createTaskGroup("Compressor1")
            ->addTask(std::move(compressor1));

        pipeline->createTaskGroup("Compressor2")
            ->addTask(std::move(compressor2));

        pipeline->createTaskGroup("Compressor3")
            ->addTask(std::move(compressor3));

        pipeline->createTaskGroup("Compressor4")
            ->addTask(std::move(compressor4));

        // Ask the pipeline to create a thread for the ROB.
        pipeline->createTaskGroup("ReorderBuffer")
            ->addTask(std::move(rob));

        // Note that there is no API to put the AsyncDatabaseWriter on a
        // thread (TaskGroup) of your choice since they all have to run
        // on the shared database thread (SimDB rule for performance).

        return pipeline;
    }

    void process(TestData&& data)
    {
        auto idx = rand() % pool_input_queues_.size();
        auto q = pool_input_queues_.at(idx);
        q->emplace(std::move(data));
    }

private:
    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::vector<simdb::ConcurrentQueue<TestData>*> pool_input_queues_;
};

REGISTER_SIMDB_APPLICATION(CompressionPool);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(CompressionPool::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<CompressionPool>();
    std::vector<TestData> sent_data;
    for (int64_t tick = 1; tick <= 100000; ++tick)
    {
        auto data = TestData::createRandom(tick);
        sent_data.push_back(data);
        app->process(std::move(data));
    }

    // Finish...
    app_mgr.postSimLoopTeardown();

    // Validate...
    auto query = db_mgr.createQuery("CompressedData");

    int64_t tick;
    query->select("Tick", tick);

    std::vector<char> blob;
    query->select("CompressedBytes", blob);

    auto result_set = query->getResultSet();
    for (int64_t tick = 1; tick <= 100000; ++tick)
    {
        auto success = result_set.getNextRecord();
        EXPECT_TRUE(success);

        if (success)
        {
            std::vector<uint64_t> decompressed;
            simdb::decompressData(blob, decompressed);

            EXPECT_EQUAL(tick, sent_data[tick-1].tick);
            EXPECT_EQUAL(decompressed, sent_data[tick-1].data);
        }
    }

    EXPECT_FALSE(result_set.getNextRecord());

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
