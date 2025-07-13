// clang-format off

#include "simdb/pipeline/Thread.hpp"
#include "simdb/pipeline/elements/DatabaseQueue.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

#include <tbb/flow_graph.h>

TEST_INIT;

int main()
{
    tbb::flow::graph g;

    // Design a schema to hold compressed stats values.
    simdb::Schema schema;
    using dt = simdb::SqlDataType;

    auto& stat_blob_tbl = schema.addTable("StatBlobs");
    stat_blob_tbl.addColumn("Tick", dt::int64_t);
    stat_blob_tbl.addColumn("StatBlob", dt::blob_t);

    simdb::DatabaseManager db_mgr("test.db", true);
    db_mgr.appendSchema(schema);

    // Design a pipeline that accepts std::vector<double> stats values,
    // compresses them into std::vector<char> buffers, then writes them
    // to the database.

    using StatsVector = std::vector<double>;
    using StatsVectorPtr = std::shared_ptr<StatsVector>;
    using TimestampedStats = std::pair<size_t, StatsVectorPtr>;

    using CompressedBytes = std::vector<char>;
    using CompressedBytesPtr = std::shared_ptr<CompressedBytes>;
    using TimestampedBytes = std::pair<size_t, CompressedBytesPtr>;

    simdb::pipeline::DatabaseThread db_thread(&db_mgr);

    simdb::pipeline::DatabaseQueue<TimestampedBytes, void> db_queue(db_thread,
        [](TimestampedBytes&& bytes, simdb::DatabaseManager* db_mgr)
        {
            // We designed this pipeline to be fast, but that means that the
            // blobs could be out of order as they were compressed across many
            // threads. We could have designed the pipeline to rearrange the
            // packets en route to the DB thread too (slower pipeline, smaller
            // database).
            db_mgr->INSERT(SQL_TABLE("StatBlobs"),
                           SQL_COLUMNS("Tick", "StatBlob"),
                           SQL_VALUES(bytes.first, *bytes.second));
        });

    tbb::flow::function_node<TimestampedStats, TimestampedBytes> compressor(g, tbb::flow::unlimited,
        [](TimestampedStats stats)
        {
            TimestampedBytes bytes = std::make_pair(stats.first, std::make_shared<CompressedBytes>());
            simdb::compressData(*stats.second, *bytes.second);
            return bytes;
        });

    tbb::flow::function_node<TimestampedBytes, tbb::flow::continue_msg> sqlite(g, tbb::flow::serial,
        [&db_queue]
        (TimestampedBytes bytes) mutable
        {
            db_queue.process(std::move(bytes));
            return tbb::flow::continue_msg{};
        });

    tbb::flow::make_edge(compressor, sqlite);
    db_thread.open();

    constexpr size_t STEPS = 10000;
    for (size_t i = 1; i <= STEPS; ++i)
    {
        // [1]
        // [2,2]
        // [3,3,3]
        // ...
        auto stats = std::make_shared<StatsVector>(i, (double)i);
        auto in = std::make_pair(i, stats);
        compressor.try_put(in);
    }

    g.wait_for_all();
    db_thread.close();
    db_thread.printPerfReport(std::cout);

    auto query = db_mgr.createQuery("StatBlobs");

    std::vector<char> bytes;
    query->select("StatBlob", bytes);
    EXPECT_EQUAL(query->count(), STEPS);

    query->orderBy("Tick", simdb::QueryOrder::ASC);

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
