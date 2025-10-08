// clang-format off

#include "simdb/pipeline/PipelineManager.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/CircularBuffer.hpp"
#include "simdb/pipeline/elements/AsyncDbWriter.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/pipeline/Queue.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "SimDBTester.hpp"

/// This test verifies the behavior of all SimDB-provided pipeline elements.
/// See the examples directory to see these elements combined together in
/// larger pipelines and SimDB apps.

/// ------ simdb::pipeline::Function --------------------------------------------
void DoublerFunctionTask(int&& val, simdb::ConcurrentQueue<int>& out, bool /*force*/)
{
    out.push(val * 2);
}

void TestPipelineFunction(simdb::DatabaseManager* db_mgr)
{
    auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr, "Functions");

    // Create a function task using a free function.
    auto func1 = simdb::pipeline::createTask<simdb::pipeline::Function<int, int>>(DoublerFunctionTask);

    // Create a function task using a lambda.
    auto func2 = simdb::pipeline::createTask<simdb::pipeline::Function<int, int>>(
        [](int&& val, simdb::ConcurrentQueue<int>& out, bool /*force*/)
        {
            out.push(val * 2);
        }
    );

    // Connect the function outputs.
    simdb::pipeline::Queue<int> func1_outq, func2_outq;
    func1->setOutputQueue(&func1_outq);
    func2->setOutputQueue(&func2_outq);

    simdb::ConcurrentQueue<int>& func1_out = func1_outq.get();
    simdb::ConcurrentQueue<int>& func2_out = func2_outq.get();

    // Get the function inputs.
    auto& func1_in = dynamic_cast<simdb::pipeline::Queue<int>*>(func1->getInputQueue())->get();
    auto& func2_in = dynamic_cast<simdb::pipeline::Queue<int>*>(func2->getInputQueue())->get();

    // Request the processing thread.
    pipeline->createTaskGroup("Doublers")
        ->addTask(std::move(func1))
        ->addTask(std::move(func2));

    // Run data through the pipeline.
    simdb::pipeline::PipelineManager pipeline_mgr(db_mgr);
    pipeline_mgr.addPipeline(std::move(pipeline));
    pipeline_mgr.openPipelines();

    for (int val = 0; val < 1000; ++val)
    {
        func1_in.push(val);
        func2_in.push(val);
        if (val % 100 == 50)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    pipeline_mgr.postSimLoopTeardown();

    // Validate.
    auto validate = [](simdb::ConcurrentQueue<int>& queue, int expected_val)
    {
        int actual_val = 0;
        if (queue.try_pop(actual_val))
        {
            EXPECT_EQUAL(expected_val, actual_val);
        }
        else
        {
            EXPECT_EQUAL(std::string(), "Fail: No more data available");
        }
    };

    for (int expected_val = 0; expected_val < 2000; expected_val += 2)
    {
        validate(func1_out, expected_val);
        validate(func2_out, expected_val);
    }
}

/// ------ simdb::pipeline::Buffer ----------------------------------------------
void TestPipelineBuffer(simdb::DatabaseManager* db_mgr, bool flush_partial)
{
    auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr, "Buffers");

    // Create a buffer task to hold 100 ints before releasing all of them.
    auto buffer = simdb::pipeline::createTask<simdb::pipeline::Buffer<int>>(100, flush_partial);

    // Connect the buffer output.
    simdb::pipeline::Queue<std::vector<int>> buffer_outq;
    buffer->setOutputQueue(&buffer_outq);
    simdb::ConcurrentQueue<std::vector<int>>& buffer_out = buffer_outq.get();

    // Get the buffer input.
    auto& buffer_in = dynamic_cast<simdb::pipeline::Queue<int>*>(buffer->getInputQueue())->get();

    // Request the processing thread.
    pipeline->createTaskGroup("Buffer")
        ->addTask(std::move(buffer));

    // Run data through the pipeline.
    simdb::pipeline::PipelineManager pipeline_mgr(db_mgr);
    pipeline_mgr.addPipeline(std::move(pipeline));
    pipeline_mgr.openPipelines();

    // Go over a "clean" boundary (mod 100) to verify the behavior of
    // buffers directed to flush partial buffers on simulation teardown.
    for (int val = 0; val < 1007; ++val)
    {
        buffer_in.push(val);
        if (val % 100 == 50)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    auto wait_avail = [&](size_t num_tries = 5)
    {
        while (buffer_out.size() == 0 && num_tries-- > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    };

    // Validate.
    auto validate = [&](simdb::ConcurrentQueue<std::vector<int>>& queue, int expected_first_val, int expected_num_vals)
    {
        wait_avail();

        std::vector<int> actual_buffer;
        if (queue.try_pop(actual_buffer))
        {
            EXPECT_EQUAL(actual_buffer.size(), expected_num_vals);
            for (size_t i = 0; i < actual_buffer.size(); ++i)
            {
                EXPECT_EQUAL(actual_buffer[i], expected_first_val + i);
            }
        }
        else
        {
            EXPECT_EQUAL(std::string(), "Fail: No more data available");
        }
    };

    for (int start = 0; start < 1000; start += 100)
    {
        validate(buffer_out, start, 100);
    }

    // Verify the extra data (partially filled vectors) were not released
    // from the buffer.
    std::vector<int> remaining;
    EXPECT_FALSE(buffer_out.try_pop(remaining));

    pipeline_mgr.postSimLoopTeardown();

    if (flush_partial)
    {
        validate(buffer_out, 1000, 7);
    }
}

/// ------ simdb::CircularBuffer ------------------------------------------------
void TestCircularBuffer()
{
    simdb::CircularBuffer<int, 10> circbuf;
    for (int val = 0; val < 10; ++val)
    {
        EXPECT_FALSE(circbuf.full());
        circbuf.push(1);
    }
    EXPECT_TRUE(circbuf.full());

    for (int val = 0; val < 10; ++val)
    {
        EXPECT_FALSE(circbuf.empty());
        circbuf.pop();
    }
    EXPECT_TRUE(circbuf.empty());
}

/// ------ simdb::pipeline::CircularBuffer --------------------------------------
void TestPipelineCircularBuffer(simdb::DatabaseManager* db_mgr)
{
    auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr, "CircularBuffers");

    // Create a circular buffer task to hold 10 ints max and release the oldest
    // down the pipeline to make room for new incoming data.
    auto circbuf = simdb::pipeline::createTask<simdb::CircularBuffer<int, 10>>();

    // Connect the circular buffer output.
    simdb::pipeline::Queue<int> circbuf_outq;
    circbuf->setOutputQueue(&circbuf_outq);
    simdb::ConcurrentQueue<int>& circbuf_out = circbuf_outq.get();

    // Get the circular buffer input.
    auto& circbuf_in = dynamic_cast<simdb::pipeline::Queue<int>*>(circbuf->getInputQueue())->get();

    // Request the processing thread.
    pipeline->createTaskGroup("CircularBuffer")
        ->addTask(std::move(circbuf));

    // Run data through the pipeline.
    simdb::pipeline::PipelineManager pipeline_mgr(db_mgr);
    pipeline_mgr.addPipeline(std::move(pipeline));
    pipeline_mgr.openPipelines();

    // Write 15 values and ensure only 5 came out (oldest, 0 through 4).
    for (int val = 0; val < 15; ++val)
    {
        circbuf_in.push(val);
    }

    auto wait_avail = [&](size_t min_avail = 1, size_t num_tries = 10)
    {
        auto num_avail = circbuf_out.size();
        while (num_avail < min_avail && num_tries-- > 0)
        {
            std::cout << "  - Waiting on " << min_avail << " values, have " << num_avail << " so far\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            num_avail = circbuf_out.size();
        }
    };

    // Wait for the oldest (0-4) to arrive
    wait_avail();

    auto validate = [&](int expected_val)
    {
        int actual_val = 0;
        bool expect_avail = expected_val >= 0;

        if (expect_avail)
        {
            // Wait for any data to arrive
            wait_avail();

            if (circbuf_out.try_pop(actual_val))
            {
                EXPECT_EQUAL(actual_val, expected_val);
            }
            else
            {
                EXPECT_EQUAL(std::string(), "Fail: No more data available");
            }
        }
        else if (circbuf_out.size())
        {
            EXPECT_EQUAL(std::string(), "Fail: Data available when there shouldn't be");
        }
    };

    validate(0);
    validate(1);
    validate(2);
    validate(3);
    validate(4);
    validate(-1);

    // Flush the pipeline instead of just sleeping the thread. Now all data
    // should have come through the circular buffer.
    pipeline_mgr.postSimLoopTeardown();

    for (int expected_val = 5; expected_val < 15; ++expected_val)
    {
        validate(expected_val);
    }
    validate(-1);
}

/// ------ simdb::pipeline::AsyncDatabaseWriter ---------------------------------
void TestAsyncDatabaseWriter(simdb::DatabaseManager* db_mgr)
{
    simdb::pipeline::PipelineManager pipeline_mgr(db_mgr);
    auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr, "AsyncWriters");

    struct TestData
    {
        int intval;
        double dblval;
        std::string strval;
        std::vector<int16_t> blobval;

        static TestData createRandom()
        {
            TestData test_data;
            test_data.intval = rand() % 100;
            test_data.dblval = rand() % 100 * 3.14;
            test_data.strval = std::to_string(rand() % 100);

            test_data.blobval.resize(rand() % 100);
            for (auto& val : test_data.blobval)
            {
                val = rand() % 100;
            }
            return test_data;
        }
    };

    struct TestDataApp
    {
        static void defineSchema(simdb::Schema& schema)
        {
            using dt = simdb::SqlDataType;
            auto& tbl = schema.addTable("TestData");
            tbl.addColumn("IntVal", dt::int32_t);
            tbl.addColumn("DblVal", dt::double_t);
            tbl.addColumn("StrVal", dt::string_t);
            tbl.addColumn("BlobVal", dt::blob_t);
        }
    };

    if (!db_mgr->getSchema().hasTable("TestData"))
    {
        simdb::Schema schema;
        TestDataApp::defineSchema(schema);
        db_mgr->appendSchema(schema);
    }

    // Create an async database writer to "batch process" all our data
    // on the dedicated database thread.
    auto writer = pipeline_mgr.getAsyncDatabaseAccessor()->createAsyncWriter<TestDataApp, TestData, void>(
        [](TestData&& test_data,
           simdb::pipeline::AppPreparedINSERTs* tables,
           bool /*force*/)
        {
            auto inserter = tables->getPreparedINSERT("TestData");
            inserter->setColumnValue(0, test_data.intval);
            inserter->setColumnValue(1, test_data.dblval);
            inserter->setColumnValue(2, test_data.strval);
            inserter->setColumnValue(3, test_data.blobval);
            inserter->createRecord();
        }
    );

    // Get the buffer input.
    auto& test_data_in = dynamic_cast<simdb::pipeline::Queue<TestData>*>(writer->getInputQueue())->get();

    // Run data through the pipeline.
    pipeline_mgr.addPipeline(std::move(pipeline));
    pipeline_mgr.openPipelines();

    std::vector<TestData> sent_test_data;
    for (int i = 0; i < 1000; ++i)
    {
        auto test_data = TestData::createRandom();
        sent_test_data.push_back(test_data);
        test_data_in.emplace(std::move(test_data));
    }

    pipeline_mgr.postSimLoopTeardown();

    // Validate.
    auto query = db_mgr->createQuery("TestData");

    TestData actual_test_data;
    query->select("IntVal", actual_test_data.intval);
    query->select("DblVal", actual_test_data.dblval);
    query->select("StrVal", actual_test_data.strval);
    query->select("BlobVal", actual_test_data.blobval);

    auto result_set = query->getResultSet();
    for (const auto& expected_test_data : sent_test_data)
    {
        auto found = result_set.getNextRecord();
        EXPECT_TRUE(found);
        if (found)
        {
            EXPECT_EQUAL(actual_test_data.intval, expected_test_data.intval);
            EXPECT_EQUAL(actual_test_data.dblval, expected_test_data.dblval);
            EXPECT_EQUAL(actual_test_data.strval, expected_test_data.strval);
            EXPECT_EQUAL(actual_test_data.blobval, expected_test_data.blobval);
        }
    }
}

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);

    TestPipelineFunction(&db_mgr);
    TestPipelineBuffer(&db_mgr, true);
    //TestPipelineBuffer(&db_mgr, false);
    TestCircularBuffer();
    TestPipelineCircularBuffer(&db_mgr);
    TestAsyncDatabaseWriter(&db_mgr);

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
