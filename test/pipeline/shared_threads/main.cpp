// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/elements/Buffer.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/DatabaseQueue.hpp"
#include "simdb/utils/RunningMean.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/TinyStrings.hpp"
#include "SimDBTester.hpp"

// This test creates 4 separate SimDB apps with competing requirements:
//
//    App   #Pre-DB   DB?   #Post-DB
//    -----------------------------------
//    1     2         N     0
//    2     3         Y     1
//    3     0         Y     2
//    4     0         Y     0
//

// First app:
//   Number of pre-database TaskGroups:     2
//   Uses a database TaskGroup?             N
//   Number of post-database TaskGroups:    0
class App1 : public simdb::App
{
public:
    static constexpr auto NAME = "app-1";
    App1(simdb::DatabaseManager* db_mgr) : db_mgr_(db_mgr) {}
    ~App1() noexcept = default;

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Thread 1 task
        auto doubler_task = simdb::pipeline::createTask<simdb::pipeline::Function<uint64_t, uint64_t>>(
            [](uint64_t&& in, simdb::ConcurrentQueue<uint64_t>& out)
            {
                out.push(in * 2);
            }
        );

        // Thread 2 task
        auto tripler_task = simdb::pipeline::createTask<simdb::pipeline::Function<uint64_t, void>>(
            [this](uint64_t&& in) { final_pipeline_values_.push_back(in); }
        );

        // Connect tasks -------------------------------------------------------------------
        *doubler_task >> *tripler_task;

        // Get the pipeline input (head) ---------------------------------------------------
        pipeline_head_ = doubler_task->getTypedInputQueue<uint64_t>();

        // Assign threads (task groups) ----------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("PreDB_Thread1")
            ->addTask(std::move(doubler_task));

        // Thread 2:
        pipeline->createTaskGroup("PreDB_Thread2")
            ->addTask(std::move(tripler_task));

        return pipeline;
    }

    void process(uint64_t val)
    {
        pipeline_head_->push(val);
    }

private:
    simdb::ConcurrentQueue<uint64_t>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::vector<uint64_t> final_pipeline_values_;
};

// Second app:
//   Number of pre-database TaskGroups:     3
//   Uses a database TaskGroup?             Y
//   Number of post-database TaskGroups:    1
class App2 : public simdb::App
{
public:
    static constexpr auto NAME = "app-2";
    App2(simdb::DatabaseManager* db_mgr) : db_mgr_(db_mgr) {}
    ~App2() noexcept = default;

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;
        auto& dp_tbl = schema.addTable("App2Data");
        dp_tbl.addColumn("IntVal", dt::int64_t);
        return true;
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Thread 1 task
        auto doubler_task = simdb::pipeline::createTask<simdb::pipeline::Function<uint64_t, uint64_t>>(
            [](uint64_t&& in, simdb::ConcurrentQueue<uint64_t>& out)
            {
                out.push(in * 2);
            }
        );

        // Thread 2 task
        auto tripler_task = simdb::pipeline::createTask<simdb::pipeline::Function<uint64_t, uint64_t>>(
            [](uint64_t&& in, simdb::ConcurrentQueue<uint64_t>& out)
            {
                out.push(in * 3);
            }
        );

        // Thread 3 task
        auto halver_task = simdb::pipeline::createTask<simdb::pipeline::Function<uint64_t, uint64_t>>(
            [](uint64_t&& in, simdb::ConcurrentQueue<uint64_t>& out)
            {
                out.push(in >> 1);
            }
        );

        // Thread 4 task (database thread)
        auto db_task = simdb::pipeline::createTask<simdb::pipeline::DatabaseQueue<uint64_t, int>>(
            [](uint64_t&& in, simdb::ConcurrentQueue<int>& out, simdb::DatabaseManager* db_mgr)
            {
                auto record = db_mgr->INSERT(
                    SQL_TABLE("App2Data"),
                    SQL_COLUMNS("IntVal"),
                    SQL_VALUES(in));

                out.push(record->getId());
            }
        );

        // Thread 5 task
        auto stdout_task = simdb::pipeline::createTask<simdb::pipeline::Function<int, void>>(
            [](int&& id)
            {
                std::cout << "Committed record with ID " << id << "\n";
            }
        );

        // Connect tasks -------------------------------------------------------------------
        *doubler_task >> *tripler_task >> *halver_task >> *db_task >> *stdout_task;

        // Get the pipeline input (head) ---------------------------------------------------
        pipeline_head_ = doubler_task->getTypedInputQueue<uint64_t>();

        // Assign threads (task groups) ----------------------------------------------------
        // Thread 1
        pipeline->createTaskGroup("PreDB_Thread1")
            ->addTask(std::move(doubler_task));

        // Thread 2
        pipeline->createTaskGroup("PreDB_Thread2")
            ->addTask(std::move(tripler_task));

        // Thread 3
        pipeline->createTaskGroup("PreDB_Thread3")
            ->addTask(std::move(halver_task));

        // Thread 4
        pipeline->createTaskGroup("Database_Thread")
            ->addTask(std::move(db_task));

        // Thread 5
        pipeline->createTaskGroup("PostDB_Thread1")
            ->addTask(std::move(stdout_task));

        return pipeline;
    }

    void process(uint64_t val)
    {
        pipeline_head_->push(val);
    }

private:
    simdb::ConcurrentQueue<uint64_t>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
};

// Third app:
//   Number of pre-database TaskGroups:     0
//   Uses a database TaskGroup?             Y
//   Number of post-database TaskGroups:    2
class App3 : public simdb::App
{
public:
    static constexpr auto NAME = "app-3";
    App3(simdb::DatabaseManager* db_mgr) : db_mgr_(db_mgr) {}
    ~App3() noexcept = default;

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;
        auto& dp_tbl = schema.addTable("App3Data");
        dp_tbl.addColumn("DataBlob", dt::blob_t);
        return true;
    }

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Thread 1 tasks (database thread)

        using PreBufferIn = uint64_t;
        using PreBufferOut = std::vector<PreBufferIn>;

        auto buffer_task = simdb::pipeline::createTask<simdb::pipeline::Buffer<PreBufferIn>>(100);

        using ZlibIn = PreBufferOut;
        using ZlibOut = std::vector<char>;

        auto zlib_task = simdb::pipeline::createTask<simdb::pipeline::Function<ZlibIn, ZlibOut>>(
            [](ZlibIn&& in, simdb::ConcurrentQueue<ZlibOut>& out)
            {
                ZlibOut compressed;
                simdb::compressData(in, compressed);
                out.emplace(std::move(compressed));
            }
        );

        using DatabaseIn = ZlibOut;
        using DatabaseOut = std::pair<int, size_t>; // Database record ID, # compressed bytes

        auto db_task = simdb::pipeline::createTask<simdb::pipeline::DatabaseQueue<DatabaseIn, DatabaseOut>>(
            [](DatabaseIn&& in, simdb::ConcurrentQueue<DatabaseOut>& out, simdb::DatabaseManager* db_mgr)
            {
                auto record = db_mgr->INSERT(
                    SQL_TABLE("App3Data"),
                    SQL_COLUMNS("DataBlob"),
                    SQL_VALUES(in));

                DatabaseOut o = std::make_pair(record->getId(), in.size());
                out.emplace(std::move(o));
            }
        );

        // Thread 2 task

        using TallyIn = DatabaseOut;
        using TallyOut = std::pair<size_t, size_t>; // Total records created, avg # bytes

        auto running_tally_task = simdb::pipeline::createTask<simdb::pipeline::Function<TallyIn, TallyOut>>(
            [this](TallyIn&& in, simdb::ConcurrentQueue<TallyOut>& out) mutable
            {
                ++num_db_records_;
                running_mean_.add(in.second);

                TallyOut o = std::make_pair(num_db_records_, size_t(running_mean_.mean()));
                out.emplace(std::move(o));
            }
        );

        // Thread 3 task

        using ReportIn = TallyOut;
        using ReportOut = void;

        auto report_task = simdb::pipeline::createTask<simdb::pipeline::Function<ReportIn, ReportOut>>(
            [this](ReportIn&& in)
            {
                final_report_ = in;
            }
        );

        // Connect tasks -------------------------------------------------------------------
        *buffer_task >> *zlib_task >> *db_task >> *running_tally_task >> *report_task;

        // Get the pipeline input (head) ---------------------------------------------------
        pipeline_head_ = buffer_task->getTypedInputQueue<uint64_t>();

        // Assign threads (task groups) ----------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("Database_Thread")
            ->addTask(std::move(buffer_task))
            ->addTask(std::move(zlib_task))
            ->addTask(std::move(db_task));

        // Thread 2:
        pipeline->createTaskGroup("PostDB_Thread1")
            ->addTask(std::move(running_tally_task));

        // Thread 3:
        pipeline->createTaskGroup("PostDB_Thread2")
            ->addTask(std::move(report_task));

        return pipeline;
    }

    void process(uint64_t val)
    {
        pipeline_head_->push(val);
    }

private:
    simdb::ConcurrentQueue<uint64_t>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::RunningMean running_mean_;
    uint64_t num_db_records_ = 0;
    std::pair<size_t, size_t> final_report_; // Total records created, avg # bytes
};

// Fourth app:
//   Number of pre-database TaskGroups:     0
//   Uses a database TaskGroup?             Y
//   Number of post-database TaskGroups:    0
class App4 : public simdb::App
{
public:
    static constexpr auto NAME = "app-4";
    App4(simdb::DatabaseManager* db_mgr) : db_mgr_(db_mgr), tiny_strings_(db_mgr) {}
    ~App4() noexcept = default;

    std::unique_ptr<simdb::pipeline::Pipeline> createPipeline() override
    {
        auto pipeline = std::make_unique<simdb::pipeline::Pipeline>(db_mgr_, NAME);

        // Thread 1 task
        auto db_task = simdb::pipeline::createTask<simdb::pipeline::DatabaseQueue<std::string, void>>(
            [this](std::string&& in, simdb::DatabaseManager*) mutable
            {
                tiny_strings_.insert(in);
                tiny_strings_.serialize();
            }
        );

        // Get the pipeline input (head) ---------------------------------------------------
        pipeline_head_ = db_task->getTypedInputQueue<std::string>();

        // Assign threads (task groups) ----------------------------------------------------
        // Thread 1:
        pipeline->createTaskGroup("Database")
            ->addTask(std::move(db_task));

        return pipeline;
    }

    void process(uint64_t val)
    {
        pipeline_head_->push(std::to_string(val));
    }

private:
    simdb::ConcurrentQueue<std::string>* pipeline_head_ = nullptr;
    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::TinyStrings<true> tiny_strings_;
};

REGISTER_SIMDB_APPLICATION(App1);
REGISTER_SIMDB_APPLICATION(App2);
REGISTER_SIMDB_APPLICATION(App3);
REGISTER_SIMDB_APPLICATION(App4);

TEST_INIT;

int main(int argc, char** argv)
{
    DB_INIT;

    simdb::DatabaseManager db_mgr("test.db");
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(App1::NAME);
    app_mgr.enableApp(App2::NAME);
    app_mgr.enableApp(App3::NAME);
    app_mgr.enableApp(App4::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    auto app1 = app_mgr.getApp<App1>();
    auto app2 = app_mgr.getApp<App2>();
    auto app3 = app_mgr.getApp<App3>();
    auto app4 = app_mgr.getApp<App4>();
    for (size_t i = 1; i <= 10000; ++i)
    {
        const uint64_t rndval = rand() % 100;
        app1->process(rndval);
        app2->process(rndval);
        app3->process(rndval);
        app4->process(rndval);
    }

    // Finish...
    app_mgr.postSim();
    app_mgr.teardown();
    app_mgr.destroy();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
