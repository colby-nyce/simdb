// clang-format off

#include "SimDBTester.hpp"
#include "TestData.hpp"
#include "TestSchema.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

/// This test covers basic DELETE functionality for SimDB.

int main()
{
    simdb::Schema schema;
    using dt = simdb::SqlDataType;

    auto& tbl = schema.addTable("TickRanges");
    tbl.addColumn("StartTick", dt::int64_t);
    tbl.addColumn("EndTick", dt::int64_t);

    simdb::DatabaseManager db_mgr("test.db", true);
    EXPECT_TRUE(db_mgr.appendSchema(schema));

    // Build this table of tick ranges:
    //
    //   StartTick      EndTick
    //   --------------------------------
    //   1              100
    //   101            200
    //   201            500
    //   501            700
    //   701            750
    //   751            800
    //   801            850
    //   851            900
    //   901            1000
    auto insert_range = [&db_mgr](uint64_t start, uint64_t end)
    {
        auto record = db_mgr.INSERT(
            SQL_TABLE("TickRanges"),
            SQL_COLUMNS("StartTick", "EndTick"),
            SQL_VALUES(start, end));

        return record->getId();
    };

    const auto range1_id = insert_range(1,   100);
    const auto range2_id = insert_range(101, 200);
    const auto range3_id = insert_range(201, 500);
    const auto range4_id = insert_range(501, 700);
    const auto range5_id = insert_range(701, 750);
    const auto range6_id = insert_range(751, 800);
    const auto range7_id = insert_range(801, 850);
    const auto range8_id = insert_range(851, 900);
    const auto range9_id = insert_range(901, 1000);

    // Negative test: findRecord() with bad ID.
    const auto bad_id = 404;
    auto bad_record = db_mgr.findRecord("TickRanges", bad_id);
    EXPECT_EQUAL(bad_record.get(), nullptr);

    // Verify we can get a SqlRecord for a valid ID.
    auto record1 = db_mgr.findRecord("TickRanges", range1_id);
    EXPECT_NOTEQUAL(record1.get(), nullptr);
    EXPECT_EQUAL(record1->getId(), range1_id);
    EXPECT_EQUAL(record1->getPropertyInt64("StartTick"), 1);
    EXPECT_EQUAL(record1->getPropertyInt64("EndTick"), 100);

    // Delete a record and verify that it is gone. You can do this through the DatabaseManager
    // for large deletes or through SqlRecord for single-record deletes. We will test deletes
    // via SqlRecord first.
    EXPECT_TRUE(record1->removeFromTable());

    // Removing it again should return false.
    EXPECT_FALSE(record1->removeFromTable());

    // DatabaseManager should not be able to find it.
    EXPECT_EQUAL(db_mgr.findRecord("TickRanges", range1_id).get(), nullptr);

    // Now test deleting a single record through the DatabaseManager.
    auto record2 = db_mgr.findRecord("TickRanges", range2_id);
    EXPECT_NOTEQUAL(record2.get(), nullptr);
    EXPECT_EQUAL(record2->getId(), range2_id);
    EXPECT_TRUE(db_mgr.removeRecordFromTable("TickRanges", range2_id));

    // Removing it again should return false.
    EXPECT_FALSE(db_mgr.removeRecordFromTable("TickRanges", range2_id));

    // We are down to 7 records. Verify that we can delete all of them at once.
    auto num_removed = db_mgr.removeAllRecordsFromTable("TickRanges");
    EXPECT_EQUAL(num_removed, 7);

    // Removing them again should not affect the table.
    num_removed = db_mgr.removeAllRecordsFromTable("TickRanges");
    EXPECT_EQUAL(num_removed, 0);

    // Note that you can also delete using WHERE constraints, but that
    // is shown in Query.cpp

    // -Werror=unused-variable
    (void)range3_id;
    (void)range4_id;
    (void)range5_id;
    (void)range6_id;
    (void)range7_id;
    (void)range8_id;
    (void)range9_id;

    REPORT_ERROR;
    return ERROR_CODE;
}
