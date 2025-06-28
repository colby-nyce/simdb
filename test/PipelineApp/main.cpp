// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "simdb/apps/DatabaseQueue.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

#include <tbb/flow_graph.h>

// This test shows how to configure and build a pipeline for SimDB apps.
// It is the same test as TBB/main.cpp but configured as an app instead
// of standalone code.

using StatsVector = std::vector<double>;
using StatsVectorPtr = std::shared_ptr<StatsVector>;
using TaggedStats = std::pair<uint64_t, StatsVectorPtr>;

using CompressedBytes = std::vector<char>;
using CompressedBytesPtr = std::shared_ptr<CompressedBytes>;
using TaggedBytes = std::pair<uint64_t, CompressedBytesPtr>;

class StatBlobSerializer : public simdb::App
{
public:
    static constexpr auto NAME = "stat-blobs";

    StatBlobSerializer(simdb::DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
        db_thread_ = std::make_unique<simdb::DatabaseQueue<CompressedBytesPtr, true>>(
            *db_mgr_,
            [](simdb::DatabaseManager& db_mgr, CompressedBytesPtr&& bytes)
            {
                db_mgr.INSERT(SQL_TABLE("StatBlobs"),
                              SQL_COLUMNS("StatBlob"),
                              SQL_VALUES(*bytes));
            });
    }

    bool defineSchema(simdb::Schema& schema) override
    {
        using dt = simdb::SqlDataType;
        auto& stat_blob_tbl = schema.addTable("StatBlobs");
        stat_blob_tbl.addColumn("StatBlob", dt::blob_t);

        return true;
    }

    void postInit(int argc, char** argv) override
    {
        (void)argc;
        (void)argv;
        createPipeline_();
    }

    void postSim() override
    {
        pipeline_->wait_for_all();
    }

    void teardown() override
    {
        db_thread_->stop();
    }

    void process(StatsVector&& stats)
    {
        process(std::make_shared<StatsVector>(std::move(stats)));
    }

    void process(const StatsVector& stats)
    {
        process(std::make_shared<StatsVector>(stats));
    }

    void process(StatsVectorPtr stats)
    {
        pipeline_head_->try_put(stats);
    }

private:
    void createPipeline_()
    {
        pipeline_ = std::make_shared<tbb::flow::graph>();
        auto& g = *pipeline_;

        pipeline_head_ = std::make_unique<tbb::flow::function_node<StatsVectorPtr, TaggedStats>>(
            g, tbb::flow::serial,
            [tag = uint64_t{0}](StatsVectorPtr stats) mutable
            {
                return std::make_pair(tag++, std::move(stats));
            }
        );

        compressor_ = std::make_unique<tbb::flow::function_node<TaggedStats, TaggedBytes>>(
            g, tbb::flow::unlimited,
            [](TaggedStats tagged_stats)
            {
                TaggedBytes tagged_bytes{tagged_stats.first, std::make_shared<CompressedBytes>()};
                simdb::compressData(*tagged_stats.second, *tagged_bytes.second);
                return tagged_bytes;
            });

        sqlite_ = std::make_unique<tbb::flow::function_node<TaggedBytes, tbb::flow::continue_msg>>(
            g, tbb::flow::serial,
            [this]
            (TaggedBytes tagged_bytes) mutable
            {
                db_thread_->enqueue(std::get<0>(tagged_bytes), std::move(std::get<1>(tagged_bytes)));
                return tbb::flow::continue_msg{};
            });

        tbb::flow::make_edge(*pipeline_head_, *compressor_);
        tbb::flow::make_edge(*compressor_, *sqlite_);
    }

    simdb::DatabaseManager* db_mgr_ = nullptr;
    std::unique_ptr<simdb::DatabaseQueue<CompressedBytesPtr, true>> db_thread_;
    std::shared_ptr<tbb::flow::graph> pipeline_;
    std::unique_ptr<tbb::flow::function_node<StatsVectorPtr, TaggedStats>> pipeline_head_;
    std::unique_ptr<tbb::flow::function_node<TaggedStats, TaggedBytes>> compressor_;
    std::unique_ptr<tbb::flow::function_node<TaggedBytes, tbb::flow::continue_msg>> sqlite_;
};

REGISTER_SIMDB_APPLICATION(StatBlobSerializer);

TEST_INIT;

int main(int argc, char** argv)
{
    DB_INIT;

    simdb::AppManager app_mgr;
    app_mgr.enableApp(StatBlobSerializer::NAME);

    simdb::DatabaseManager db_mgr("test.db");

    // Setup...
    app_mgr.createEnabledApps(&db_mgr);
    app_mgr.createSchemas(&db_mgr);
    app_mgr.postInit(&db_mgr, argc, argv);

    // Simulate...
    auto serializer = app_mgr.getApp<StatBlobSerializer>(&db_mgr);
    constexpr size_t STEPS = 10000;
    for (size_t i = 1; i <= STEPS; ++i)
    {
        // [1]
        // [2,2]
        // [3,3,3]
        // ...
        StatsVector stats(i, (double)i);
        serializer->process(std::move(stats));
    }

    // Finish...
    app_mgr.postSim(&db_mgr);
    app_mgr.teardown(&db_mgr);
    app_mgr.destroy();

    // Validate...
    auto query = db_mgr.createQuery("StatBlobs");

    std::vector<char> bytes;
    query->select("StatBlob", bytes);
    EXPECT_EQUAL(query->count(), STEPS);

    auto result_set = query->getResultSet();
    for (size_t i = 1; i <= STEPS; ++i)
    {
        EXPECT_TRUE(result_set.getNextRecord());

        StatsVector actual;
        simdb::decompressData(bytes, actual);

        const StatsVector expected(i, (double)i);
        EXPECT_EQUAL(expected, actual);
    }

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
