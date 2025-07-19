// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/CircularBuffer.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "SimDBTester.hpp"

// This test creates a SimDB app with a pipeline that contains some
// pipeline elements that are provided by SimDB, as well as showing
// how you can define your own element class if the built-in ones
// aren't sufficient.
//
// The pipeline will use built-in SimDB elements:
//   - Buffer
//   - Function
//
// As well as a user-defined element:
//   - ReorderBuffer (ROB)
//
// The pipeline design:
//
// int -> ROB -> itoa() -> buffer(5) -> hashval -> circbuf(10) -> DB
//        **************************    **********************    *******************
//        Thread 1                      Thread 2                  Thread 3
//

class ReorderBuffer {};

namespace simdb::pipeline {

/// This user-defined Task element buffers incoming size_t's and only
/// emits them when it can guarantee they are back in order. A common
/// use case for this would be something like a thread pool to perform
/// parallel tasks like compression, but then you need to put them back
/// in their original order prior to a DB writer task.
template <>
class Task<ReorderBuffer> : public NonTerminalTask<size_t, size_t>
{
public:
    Task(size_t first_emitted_val)
        : next_expected_val_(first_emitted_val)
    {}

private:
    /// \brief Process one item from the queue.
    /// \note  Method cannot be public or SimDB can't guarantee thread safety.
    bool run() override
    {
        size_t val = 0;
        bool ran = false;

        if (this->input_queue_->get().try_pop(val))
        {
            ran = true;
            rob_.push(val);
        }

        if (!rob_.empty() && rob_.top() == next_expected_val_)
        {
            ++next_expected_val_;
            this->output_queue_->get().push(rob_.top());
            rob_.pop();
            ran = true;
        }
 
        return ran;
    }

    std::string getDescription_() const override
    {
        return "ReorderBuffer";
    }

    size_t next_expected_val_;
    std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>> rob_;
};

} // namespace simdb::pipeline

void ITOA(size_t&& val, simdb::ConcurrentQueue<std::string>& out)
{
    std::string o = std::to_string(val);
    out.emplace(std::move(o));
}

class PipelineElementApp : public simdb::App
{
public:
    static constexpr auto NAME = "pipeline-elements";

    PipelineElementApp(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    ~PipelineElementApp() noexcept = default;

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        // We are going to send random ints down the pipeline, do a bunch
        // of transformations on them, and generate a hash value.
        auto& dp_tbl = schema.addTable("Pipeout");
        dp_tbl.addColumn("HashVal", dt::string_t);
    }

    void postInit(int argc, char** argv) override
    {
        (void)argc;
        (void)argv;
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline(simdb::pipeline::AsyncDatabaseAccessor* db_accessor) override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Create a pipeline which takes random integers and processes them like so:
        // int -> ROB -> itoa() -> buffer(5) -> hashval -> circbuf(10) -> DB
        //        **************************    **********************    *******************
        //        Thread 1                      Thread 2                  Thread 3

        // Thread 1 tasks --------------------------------------------------------------------------

        // Task 1: Reorder buffer
        auto rob_task = simdb::pipeline::createTask<ReorderBuffer>(1);
        rob_task_ = rob_task.get();

        // *** Note the use of Function below. You can provide the function impl via a free
        // *** function, std::function, or a lambda.
        // Task 2: take size_t and return std::string (using free function)
        auto itoa_task = simdb::pipeline::createTask<simdb::pipeline::Function<size_t, std::string>>(ITOA);

        // Task 3: take std::string from prev task and output std::vector<std::string> when full
        auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<std::string>>(5);

        // Thread 2 tasks --------------------------------------------------------------------------

        // Task 4: take std::vector<std::string> from prev task and output a hashval size_t (using lambda)
        auto hashval_task = simdb::pipeline::createTask<simdb::pipeline::Function<std::vector<std::string>, size_t>>(
            [](std::vector<std::string>&& in, simdb::ConcurrentQueue<size_t>& out)
            {
                size_t seed = 0;
                std::hash<std::string> hasher;

                for (const auto& str : in)
                {
                    seed ^= hasher(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                }

                out.push(seed);
            }
        );

        // Task 5: take hashval size_t and push to a circular buffer
        auto circbuf_task = simdb::pipeline::createTask<simdb::CircularBuffer<size_t, 10>>();

        // Thread 3 tasks --------------------------------------------------------------------------

        // Task 6: take the hashval size_t emitted from the circular buffer and write to the database
        auto sqlite_task = db_accessor->createAsyncWriter<PipelineElementApp, size_t, void>(
            [](size_t&& in, simdb::pipeline::AppPreparedINSERTs* tables)
            {
                auto inserter = tables->getPreparedINSERT("Pipeout");
                inserter->setColumnValue(0, std::to_string(in));
                inserter->createRecord();
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *rob_task >> *itoa_task >> *buffer_task >> *hashval_task >> *circbuf_task >> *sqlite_task;

        // Get the pipeline input (head) -----------------------------------------------------------
        pipeline_head_ = rob_task->getTypedInputQueue<size_t>();

        // Assign threads (task groups) ------------------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("PreProcess")
            ->addTask(std::move(rob_task))
            ->addTask(std::move(itoa_task))
            ->addTask(std::move(buffer_task));

        // Thread 2:
        pipeline->createTaskGroup("Hasher")
            ->addTask(std::move(hashval_task))
            ->addTask(std::move(circbuf_task));

        return pipeline;
    }

    void process(size_t val)
    {
        pipeline_head_->push(val);
    }

    void preTeardown() override
    {
    }

    void postTeardown() override
    {
    }

private:
    simdb::ConcurrentQueue<size_t>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::pipeline::Task<ReorderBuffer>* rob_task_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(PipelineElementApp);

TEST_INIT;

std::random_device rd;
std::mt19937 g(rd());

std::vector<size_t> generateShuffledAutoIncVals(size_t starting_val, size_t num_vals)
{
    std::vector<size_t> vals;
    for (size_t val = starting_val; val < starting_val + num_vals; ++val)
    {
        vals.push_back(val);
    }

    std::shuffle(vals.begin(), vals.end(), g);
    return vals;
}

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(PipelineElementApp::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<PipelineElementApp>();

    size_t starting_val = 1;
    for (size_t i = 0; i < 100; ++i)
    {
        auto shuffled_vals = generateShuffledAutoIncVals(starting_val, 100);
        for (auto val : shuffled_vals)
        {
            app->process(val);
        }
        starting_val += 100;
    }

    // Finish...
    app_mgr.postSimLoopTeardown(true);

    // Validate...
    auto get_hash_val = [](size_t starting_val, size_t num_vals)
    {
        std::vector<std::string> itoas;
        for (size_t val = starting_val; val < starting_val + num_vals; ++val)
        {
            itoas.push_back(std::to_string(val));
        }

        size_t seed = 0;
        std::hash<std::string> hasher;
        for (const auto& str : itoas)
        {
            seed ^= hasher(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }

        return std::to_string(seed);
    };

    std::vector<std::string> expected_hash_vals;
    for (size_t i = 0; i < 1990; ++i)
    {
        starting_val = 5*i + 1;
        size_t num_vals = 5;

        auto hash_val = get_hash_val(starting_val, num_vals);
        expected_hash_vals.push_back(hash_val);
    }

    auto query = db_mgr.createQuery("Pipeout");
    EXPECT_EQUAL(query->count(), expected_hash_vals.size());

    std::string actual_hash_val;
    query->select("HashVal", actual_hash_val);

    auto result_set = query->getResultSet();
    for (auto expected_hash_val : expected_hash_vals)
    {
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(actual_hash_val, expected_hash_val);
    }
    EXPECT_FALSE(result_set.getNextRecord());

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
