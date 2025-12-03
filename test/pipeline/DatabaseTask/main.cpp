// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "simdb/pipeline/elements/Function.hpp"
#include "simdb/pipeline/elements/DatabaseTask.hpp"
#include "simdb/sqlite/PreparedINSERT.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

/// This test shows all the ways you can create async DB
/// pipeline tasks. All these are supported and will be
/// tested:
///
///    Input, Output
///    Input, no output
///    No input, Output
///    No input, no output
///
class DatabaseTaskTester : public simdb::App
{
public:
    static constexpr auto NAME = "db-task-tester";

    DatabaseTaskTester(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {}

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("CompressedArrays");
        tbl.addColumn("NumOrigBytes", dt::int32_t);
        tbl.addColumn("NumCompressedBytes", dt::int32_t);
        tbl.addColumn("CompressedBytes", dt::blob_t);
    }

    void setMaxPipelineArrays(size_t n)
    {
        max_pipeline_arrays_ = n;
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        pipeline_mgr->createPipeline(NAME);
        auto db_accessor = pipeline_mgr->getAsyncDatabaseAccessor();

        using RandLenTask = simdb::pipeline::DatabaseTask<void, int>;
        auto gen_rand_arr_len = simdb::pipeline::createTask<RandLenTask>(
            db_mgr_,
            [this]
            (simdb::ConcurrentQueue<int>& out,
             simdb::pipeline::DatabaseAccessor&,
             bool /*force_flush*/)
            {
                if (num_arrays_sent_ == max_pipeline_arrays_)
                {
                    return simdb::pipeline::RunnableOutcome::NO_OP;
                }

                auto n = rand() % 1000 + 100;
                out.push(n);
                ++num_arrays_sent_;
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        using RandArrayTask = simdb::pipeline::DatabaseTask<int, std::vector<double>>;
        auto create_array = simdb::pipeline::createTask<RandArrayTask>(
            db_mgr_,
            [](int&& n,
               simdb::ConcurrentQueue<std::vector<double>>& out,
               simdb::pipeline::DatabaseAccessor&,
               bool /*force_flush*/)
            {
                std::vector<double> arr(n);
                for (auto& val : arr)
                {
                    val = rand() % 100 * M_PI;
                }
                out.emplace(std::move(arr));
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        struct CompressionResult
        {
            size_t num_orig_bytes;
            std::vector<char> compressed_bytes;
        };

        using CompressionTask = simdb::pipeline::DatabaseTask<std::vector<double>, CompressionResult>;
        auto compress_array = simdb::pipeline::createTask<CompressionTask>(
            db_mgr_,
            [](std::vector<double>&& arr,
               simdb::ConcurrentQueue<CompressionResult>& results,
               simdb::pipeline::DatabaseAccessor&,
               bool /*force_flush*/)
            {
                CompressionResult result;
                result.num_orig_bytes = arr.size() * sizeof(double);
                simdb::compressData(arr, result.compressed_bytes);
                results.emplace(std::move(result));
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        using WriteTask = simdb::pipeline::DatabaseTask<CompressionResult, void>;
        auto write_task = simdb::pipeline::createTask<WriteTask>(
            db_mgr_,
            [this](CompressionResult&& result,
                   simdb::pipeline::DatabaseAccessor& accessor,
                   bool /*force_flush*/)
            {
                if (num_arrays_rcvd_ == max_pipeline_arrays_)
                {
                    return simdb::pipeline::RunnableOutcome::NO_OP;
                }

                auto inserter = accessor.getTableInserter<DatabaseTaskTester>("CompressedArrays");
                inserter->setColumnValue(0, (int)result.num_orig_bytes);
                inserter->setColumnValue(1, (int)result.compressed_bytes.size());
                inserter->setColumnValue(2, result.compressed_bytes);
                inserter->createRecord();

                ++num_arrays_rcvd_;
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        using ZlibPerfTask = simdb::pipeline::DatabaseTask<void, void>;
        auto report_perf_task = simdb::pipeline::createTask<ZlibPerfTask>(
            db_mgr_,
            [this, last_queried_id = 0, running_orig_bytes = 0, running_zlib_bytes = 0, last_reported_perf = 0.0]
            (simdb::pipeline::DatabaseAccessor& accessor, bool /*force_flush*/) mutable
            {
                if (num_arrays_sent_ == max_pipeline_arrays_ && num_arrays_rcvd_ == max_pipeline_arrays_)
                {
                    return simdb::pipeline::RunnableOutcome::NO_OP;
                }

                auto query = accessor.getDatabaseManager()->createQuery("CompressedArrays");
                query->addConstraintForInt("Id", simdb::Constraints::GREATER, last_queried_id);

                int id;
                query->select("Id", id);

                int orig_bytes;
                query->select("NumOrigBytes", orig_bytes);

                int zlib_bytes;
                query->select("NumCompressedBytes", zlib_bytes);

                auto results = query->getResultSet();
                while (results.getNextRecord())
                {
                    last_queried_id = id;
                    running_orig_bytes += orig_bytes;
                    running_zlib_bytes += zlib_bytes;
                }

                auto less_bytes = (running_orig_bytes - running_zlib_bytes);
                auto zlib_pct = (double)less_bytes / (double)running_orig_bytes * 100;
                if (zlib_pct != last_reported_perf)
                {
                    std::cout << "Overall compression achieved: ";
                    std::cout << std::setprecision(1) << zlib_pct << "%\n";
                }
                last_reported_perf = zlib_pct;

                return simdb::pipeline::RunnableOutcome::DID_WORK;
            }
        );

        // Connect tasks. Note that we cannot connect report_perf_task at the end
        // since it has no input queue, and write_task has no output queue.
        *gen_rand_arr_len >> *create_array >> *compress_array >> *write_task;

        // Give all these tasks to the DB thread
        db_accessor->addTasks(
            std::move(gen_rand_arr_len),
            std::move(create_array),
            std::move(compress_array),
            std::move(write_task),
            std::move(report_perf_task));
    }

    void postInit(int, char**) override
    {
        if (max_pipeline_arrays_ == 0)
        {
            throw simdb::DBException("Must set non-zero max number of arrays to process");
        }
    }

    void simulate()
    {
        while (num_arrays_sent_ != max_pipeline_arrays_)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        while (num_arrays_sent_ != num_arrays_rcvd_)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

private:
    simdb::DatabaseManager* db_mgr_ = nullptr;
    size_t max_pipeline_arrays_ = 0;
    size_t num_arrays_sent_ = 0;
    size_t num_arrays_rcvd_ = 0;
};

REGISTER_SIMDB_APPLICATION(DatabaseTaskTester);
TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(DatabaseTaskTester::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    auto& app = *app_mgr.getApp<DatabaseTaskTester>();
    app.setMaxPipelineArrays(10000);

    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Simulate...
    app.simulate();

    // Finish...
    app_mgr.postSimLoopTeardown();

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
