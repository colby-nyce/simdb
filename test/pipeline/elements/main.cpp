// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/DatabaseQueue.hpp"
#include "simdb/utils/CircularBuffer.hpp"
#include "SimDBTester.hpp"

// This test creates a SimDB app with a pipeline that contains some
// pipeline elements that are provided by SimDB, as well as showing
// how you can define your own element class if the built-in ones
// aren't sufficient.
//
// The pipeline will use built-in SimDB elements:
//   - Buffer
//   - Function
//   - DatabaseQueue
//
// As well as a user-defined element:
//   - CircularBuffer
//
// The pipeline design:
//
// int -> itoa() -> buffer(5) -> hashval -> circbuf(10) -> DB
//        *******************    **********************    *******************
//        Thread 1               Thread 2                  Thread 3
//

namespace simdb::pipeline {

template <typename DataT, size_t BufferLen>
class Task<simdb::CircularBuffer<DataT, BufferLen>> : public NonTerminalNonDatabaseTask<DataT, DataT>
{
public:
    using InputType = DataT;
    using OutputType = DataT;

    bool run() override
    {
        if (!this->output_queue_)
        {
            throw DBException("Output queue not set!");
        }

        InputType in;
        bool ran = false;
        if (this->input_queue_->get().try_pop(in))
        {
            if (circ_buf_.full())
            {
                this->output_queue_->get().emplace(std::move(circ_buf_.pop()));
            }
            circ_buf_.push(std::move(in));
            ran = true;
        }

        return ran;
    }

private:
    std::string getDescription_() const override
    {
        return "CircularBuffer<" + demangle_type<DataT>() + ", " + std::to_string(BufferLen) + ">";
    }

    simdb::CircularBuffer<InputType, BufferLen> circ_buf_;
};

} // namespace simdb::pipeline

void ITOA(size_t&& val, simdb::ConcurrentQueue<std::string>& out)
{
    std::string o = (val <= 127) ? std::string(1, static_cast<char>(val)) : "?";
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

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;

        // We are going to send random ints down the pipeline, do a bunch
        // of transformations on them, and generate a hash value.
        auto& dp_tbl = schema.addTable("Pipeout");
        dp_tbl.addColumn("HashVal", dt::int32_t);

        return true;
    }

    void postInit(int argc, char** argv) override
    {
        (void)argc;
        (void)argv;
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Create a pipeline which takes random integers and processes them like so:
        // int -> itoa() -> buffer(5) -> hashval -> circbuf(10) -> DB
        //        *******************    **********************    *******************
        //        Thread 1               Thread 2                  Thread 3

        // Thread 1 tasks --------------------------------------------------------------------------

        // *** Note the use of Function below. You can provide the function impl via a free
        // *** function, std::function, or a lambda.
        // Task 1: take size_t and return std::string (using free function)
        auto itoa_task = simdb::pipeline::createTask<simdb::pipeline::Function<size_t, std::string>>(ITOA);

        // Task 2: take std::string from prev task and output std::vector<std::string> when full
        auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<std::string>>(5);

        // Thread 2 tasks --------------------------------------------------------------------------

        // Task 3: take std::vector<std::string> from prev task and output a hashval size_t (using lambda)
        auto hashval_task = simdb::pipeline::createTask<simdb::pipeline::Function<std::vector<std::string>, size_t>>(
            [](std::vector<std::string>&& in, simdb::ConcurrentQueue<size_t>& out)
            {
                size_t seed = 0;
                std::hash<std::string> hasher;

                for (const auto& str : in) {
                    seed ^= hasher(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                }

                out.push(seed);
            }
        );

        // Task 4: take hashval size_t and push to a circular buffer (user-defined element)
        auto circbuf_task = simdb::pipeline::createTask<simdb::CircularBuffer<size_t, 10>>();

        // Thread 3 tasks --------------------------------------------------------------------------

        // Task 5: take the hashval size_t emitted from the circular buffer and write to the database
        auto sqlite_task = simdb::pipeline::createTask<simdb::pipeline::DatabaseQueue<size_t, void>>(
            [](size_t&& in, simdb::DatabaseManager* db_mgr)
            {
                db_mgr->INSERT(
                    SQL_TABLE("Pipeout"),
                    SQL_COLUMNS("HashVal"),
                    SQL_VALUES(in));
            }
        );

        // Connect tasks ---------------------------------------------------------------------------
        *itoa_task >> *buffer_task >> *hashval_task >> *circbuf_task >> *sqlite_task;

        // Get the pipeline input (head) -----------------------------------------------------------
        pipeline_head_ = itoa_task->getTypedInputQueue<size_t>();

        // Assign threads (task groups) ------------------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("PreProcess")
            ->addTask(std::move(itoa_task))
            ->addTask(std::move(buffer_task));

        // Thread 2:
        pipeline->createTaskGroup("Hasher")
            ->addTask(std::move(hashval_task))
            ->addTask(std::move(circbuf_task));

        // Thread 3:
        pipeline->createTaskGroup("Database")
            ->addTask(std::move(sqlite_task));

        return pipeline;
    }

    void process(size_t val)
    {
        pipeline_head_->push(val);
    }

    void postSim() override
    {
    }

    void teardown() override
    {
    }

private:
    simdb::ConcurrentQueue<size_t>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
};

REGISTER_SIMDB_APPLICATION(PipelineElementApp);

TEST_INIT;

int main(int argc, char** argv)
{
    DB_INIT;

    simdb::DatabaseManager db_mgr("test.db");
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(PipelineElementApp::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<PipelineElementApp>();
    for (size_t i = 1; i <= 10000; ++i)
    {
        app->process(rand() % 256);
    }

    // Finish...
    app_mgr.postSim();
    app_mgr.teardown();
    app_mgr.destroy();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
