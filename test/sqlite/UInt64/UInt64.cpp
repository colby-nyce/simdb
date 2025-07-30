// clang-format off

#include "SimDBTester.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/Random.hpp"

TEST_INIT;

/// This test covers uint64 columns for SimDB schemas. We need to take
/// special care of these values since sqlite wants to store them as
/// signed 64-bit ints.

int main()
{
    simdb::Schema schema;
    using dt = simdb::SqlDataType;

    auto& tbl = schema.addTable("DataWindows");
    tbl.addColumn("StartTick", dt::uint64_t);
    tbl.addColumn("EndTick", dt::uint64_t);
    tbl.addColumn("DataWindow", dt::blob_t);

    simdb::DatabaseManager db_mgr("test.db", true);
    EXPECT_TRUE(db_mgr.appendSchema(schema));

    auto insert_record = [&db_mgr](const std::pair<uint64_t, uint64_t>& range, const std::vector<int>& data)
    {
        return db_mgr.INSERT(
            SQL_TABLE("DataWindows"),
            SQL_COLUMNS("StartTick", "EndTick", "DataWindow"),
            SQL_VALUES(range.first, range.second, data));
    };

    auto make_range = [](uint64_t start, uint64_t end)
    {
        return std::make_pair(start, end);
    };

    constexpr auto uint32_max = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max());

    auto range1 = make_range(101, 200);
    auto range2 = make_range(uint32_max+101, uint32_max+200);
    auto range3 = make_range(uint32_max+201, uint32_max+300);
    auto range4 = make_range(uint32_max+301, uint32_max+400);
    auto range5 = make_range(UINT64_MAX, UINT64_MAX);

    auto data1 = simdb::utils::generateRandomData<int>(1000);
    auto data2 = simdb::utils::generateRandomData<int>(1000);
    auto data3 = simdb::utils::generateRandomData<int>(1000);
    auto data4 = simdb::utils::generateRandomData<int>(1000);
    auto data5 = simdb::utils::generateRandomData<int>(1000);

    auto record1 = insert_record(range1, data1);
    auto record2 = insert_record(range2, data2);
    auto record3 = insert_record(range3, data3);
    auto record4 = insert_record(range4, data4);
    auto record5 = insert_record(range5, data5);

    // Verify record set/get apis for uint64_t columns
    EXPECT_EQUAL(record2->getPropertyUInt64("StartTick"), range2.first);
    record2->setPropertyUInt64("StartTick", 50);
    EXPECT_EQUAL(record2->getPropertyUInt64("StartTick"), 50);
    record2->setPropertyUInt64("StartTick", range2.first);
    EXPECT_EQUAL(record2->getPropertyUInt64("StartTick"), range2.first);

    // Verify SqlQuery WHERE constraints for uint64_t columns
    auto query = db_mgr.createQuery("DataWindows");

    std::vector<int> data;
    query->select("DataWindow", data);

    // test 1
    query->addConstraintForUInt64("StartTick", simdb::Constraints::GREATER_EQUAL, range1.first);
    query->addConstraintForUInt64("EndTick", simdb::Constraints::LESS_EQUAL, range1.second);
    {
        auto result_set = query->getResultSet();

        // range 1
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data1);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 2
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::Constraints::GREATER_EQUAL, range2.first);
    query->addConstraintForUInt64("EndTick", simdb::Constraints::LESS_EQUAL, range2.second);
    {
        auto result_set = query->getResultSet();

        // range 2
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data2);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 3
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::Constraints::EQUAL, range3.first);
    {
        auto result_set = query->getResultSet();

        // range 3
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data3);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 4
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::Constraints::NOT_EQUAL, range3.first);
    {
        auto result_set = query->getResultSet();

        // range 1
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data1);

        // range 2
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data2);

        // range 4
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data4);

        // range 5
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data5);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 5
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::Constraints::GREATER, range2.first);
    query->addConstraintForUInt64("EndTick", simdb::Constraints::LESS, range4.second);
    {
        auto result_set = query->getResultSet();

        // range 3
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data3);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 6
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::Constraints::EQUAL, range5.first);
    query->addConstraintForUInt64("EndTick", simdb::Constraints::EQUAL, range5.second);
    {
        auto result_set = query->getResultSet();

        // range 5
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data5);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 6
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::SetConstraints::IN_SET, {range3.first});
    {
        auto result_set = query->getResultSet();

        // range 3
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data3);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 7
    query->resetConstraints();
    query->addConstraintForUInt64("StartTick", simdb::SetConstraints::NOT_IN_SET, {range3.first});
    {
        auto result_set = query->getResultSet();

        // range 1
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data1);

        // range 2
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data2);

        // range 4
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data4);

        // range 5
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(data, data5);

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // test 8
    uint64_t start_tick, end_tick;
    query->resetConstraints();
    query->select("StartTick", start_tick);
    query->select("EndTick", end_tick);
    {
        auto result_set = query->getResultSet();
        std::pair<uint64_t, uint64_t> expected_ranges[] = {range1, range2, range3, range4, range5};
        for (size_t i = 0; i < 5; ++i)
        {
            EXPECT_TRUE(result_set.getNextRecord());
            EXPECT_EQUAL(start_tick, expected_ranges[i].first);
            EXPECT_EQUAL(end_tick, expected_ranges[i].second);
        }

        // no more
        EXPECT_FALSE(result_set.getNextRecord());
    }

    REPORT_ERROR;
    return ERROR_CODE;
}
