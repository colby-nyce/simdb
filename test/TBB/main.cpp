// clang-format off

#include "simdb/apps/DatabaseQueue.hpp"
#include "simdb/utils/Compress.hpp"
#include "SimDBTester.hpp"

#include <tbb/flow_graph.h>

TEST_INIT;

int main()
{
    DB_INIT;

    tbb::flow::graph g;

    // Design a schema to hold compressed stats values.
    simdb::Schema schema;
    using dt = simdb::SqlDataType;

    auto& stat_blob_tbl = schema.addTable("StatBlobs");
    stat_blob_tbl.addColumn("StatBlob", dt::blob_t);

    simdb::DatabaseManager db_mgr("test.db");
    db_mgr.appendSchema(schema);

    // Design a pipeline that accepts std::vector<double> stats values,
    // compresses them into std::vector<char> buffers, then writes them
    // to the database. This is the architecture:
    //
    //
    //
    //                                              vec3<c>  \
    //                                              vec2<c>   \
    //   vec1<d>, vec2<d>, vec3<d>, ...   ---->     vec6<c>    \_____________ ... SQLite:
    //     |                                        vec4<c>    /                  vec1<c>
    //     |                                        vec1<c>   /                   vec2<c>
    //     |                                        vec5<c>  /                    vec3<c>
    //     |                                          |                           vec4<c>
    //     |                                          |                           vec5<c>
    //     |                                          |                           vec6<c>
    //     |                                          |                             |
    //    [Write one at a time during sim]            |                             |
    //                                               [Compress concurrently]        |
    //                                                                              |
    //                                                   [Put back in order and write]
    using StatsVector = std::vector<double>;
    using StatsVectorPtr = std::shared_ptr<StatsVector>;
    using TaggedStats = std::pair<uint64_t, StatsVectorPtr>;

    using CompressedBytes = std::vector<char>;
    using CompressedBytesPtr = std::shared_ptr<CompressedBytes>;
    using TaggedBytes = std::pair<uint64_t, CompressedBytesPtr>;

    simdb::DatabaseQueue<CompressedBytesPtr, true> db_thread(db_mgr,
        [](simdb::DatabaseManager& db_mgr, CompressedBytesPtr&& bytes)
        {
            db_mgr.INSERT(SQL_TABLE("StatBlobs"),
                          SQL_COLUMNS("StatBlob"),
                          SQL_VALUES(*bytes));
        });

    tbb::flow::function_node<StatsVectorPtr, TaggedStats> collector(g, tbb::flow::serial,
        [tag = uint64_t{0}](StatsVectorPtr stats) mutable
        {
            return std::make_pair(tag++, std::move(stats));
        });

    tbb::flow::function_node<TaggedStats, TaggedBytes> compressor(g, tbb::flow::unlimited,
        [](TaggedStats tagged_stats)
        {
            TaggedBytes tagged_bytes{tagged_stats.first, std::make_shared<CompressedBytes>()};
            simdb::compressData(*tagged_stats.second, *tagged_bytes.second);
            return tagged_bytes;
        });

    tbb::flow::function_node<TaggedBytes, tbb::flow::continue_msg> sqlite(g, tbb::flow::serial,
        [&db_thread]
        (TaggedBytes tagged_bytes) mutable
        {
            db_thread.enqueue(std::get<0>(tagged_bytes), std::move(std::get<1>(tagged_bytes)));
            return tbb::flow::continue_msg{};
        });

    tbb::flow::make_edge(collector, compressor);
    tbb::flow::make_edge(compressor, sqlite);

    constexpr size_t STEPS = 10000;
    for (size_t i = 1; i <= STEPS; ++i)
    {
        // [1]
        // [2,2]
        // [3,3,3]
        // ...
        auto stats = std::make_shared<StatsVector>(i, (double)i);
        collector.try_put(stats);
    }

    g.wait_for_all();
    db_thread.stop();

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
