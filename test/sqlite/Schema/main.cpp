// clang-format off

#include "SimDBTester.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

/// This test covers basic schema functionality for SimDB.

int main()
{
    simdb::Schema schema;
    using dt = simdb::SqlDataType;

    // Test basic schema table creation
    auto& meta_tbl = schema.addTable("SimMetadata");
    meta_tbl.addColumn("SimStart", dt::string_t);
    meta_tbl.addColumn("SimEnd", dt::string_t);
    meta_tbl.addColumn("NumInstsExecuted", dt::uint64_t);

    EXPECT_TRUE(schema.hasTable("SimMetadata"));
    EXPECT_EQUAL(&schema.getTable("SimMetadata"), &meta_tbl);
    EXPECT_EQUAL(meta_tbl.getName(), "SimMetadata");
    EXPECT_TRUE(meta_tbl.hasColumn("NumInstsExecuted"));
    EXPECT_EQUAL(meta_tbl.getColumn("NumInstsExecuted").getName(), "NumInstsExecuted");
    EXPECT_EQUAL(meta_tbl.getColumn("NumInstsExecuted").getDataType(), dt::uint64_t);

    // Negative tests for duplicates etc.
    EXPECT_THROW(schema.addTable("SimMetadata"));
    EXPECT_THROW(meta_tbl.addColumn("NumInstsExecuted", dt::uint64_t));
    EXPECT_THROW(meta_tbl.getColumn("DOES_NOT_EXIST"));
    EXPECT_THROW(schema.getTable("DOES_NOT_EXIST"));

    // Test column default values
    auto& reports_tbl = schema.addTable("Reports");
    reports_tbl.addColumn("ReportName", dt::string_t);
    reports_tbl.addColumn("StartTick", dt::uint64_t);
    reports_tbl.addColumn("EndTick", dt::uint64_t);
    reports_tbl.addColumn("StatsValues", dt::blob_t);
    reports_tbl.setColumnDefaultValue("StartTick", 0);
    reports_tbl.setColumnDefaultValue("EndTick", -1);

    // Negative tests:
    // -- Bad column name
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("DOES_NOT_EXIST", 404));
    // -- Good column name, but data type mismatch
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("ReportName", 0xdeadbeef));
    // -- Cannot set default values for blobs
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("StatsValues", simdb::SqlBlob{}));

    // Create a table with an index on a single column
    auto& stat_insts_tbl = schema.addTable("StatisticInsts");
    stat_insts_tbl.addColumn("ReportID", dt::int32_t);
    stat_insts_tbl.addColumn("StatisticName", dt::string_t);
    stat_insts_tbl.addColumn("StatisticLoc", dt::string_t);
    stat_insts_tbl.addColumn("StatisticDesc", dt::string_t);
    stat_insts_tbl.createIndexOn("ReportID");

    // Create a table with an index on multiple columns
    auto& stat_ranges_tbl = schema.addTable("StatRanges");
    stat_ranges_tbl.addColumn("StatisticInstID", dt::int32_t);
    stat_ranges_tbl.addColumn("StartTick", dt::uint64_t);
    stat_ranges_tbl.addColumn("EndTick", dt::uint64_t);
    stat_ranges_tbl.addColumn("StatValues", dt::blob_t);
    stat_ranges_tbl.createCompoundIndexOn({"StartTick", "EndTick"});

    // Negative test for bad column
    EXPECT_THROW(stat_ranges_tbl.createIndexOn("DOES_NOT_EXIST"));
    EXPECT_THROW(stat_ranges_tbl.createCompoundIndexOn({"StartTick", "DOES_NOT_EXIST"}));

    // Test appendSchema()
    simdb::Schema schema2;
    EXPECT_FALSE(schema2.hasTable("StatRanges"));
    schema2.appendSchema(schema);
    EXPECT_TRUE(schema2.hasTable("StatRanges"));

    // Negative test: try appending a schema that would result in duplicate table names
    simdb::Schema schema3;
    auto& stat_ranges_tbl_dup = schema3.addTable("StatRanges");
    stat_ranges_tbl_dup.addColumn("StartTick", dt::uint64_t);
    stat_ranges_tbl_dup.addColumn("EndTick", dt::uint64_t);
    EXPECT_THROW(schema2.appendSchema(schema3));

    // Verify that all three schemas got past their negative tests
    // and can individually be used to instantiate databases.
    simdb::DatabaseManager db_mgr("test1.db", true);
    EXPECT_NOTHROW(db_mgr.appendSchema(schema));

    simdb::DatabaseManager db_mgr2("test2.db", true);
    EXPECT_NOTHROW(db_mgr2.appendSchema(schema2));

    simdb::DatabaseManager db_mgr3("test3.db", true);
    EXPECT_NOTHROW(db_mgr3.appendSchema(schema3));

    // Verify the Schema recreation when attaching a DatabaseManager to an
    // existing database file.
    simdb::Schema schema4;
    auto& all_dtypes_tbl = schema4.addTable("AllDataTypes");
    all_dtypes_tbl.addColumn("TheInt32", dt::int32_t);
    all_dtypes_tbl.addColumn("TheUInt32", dt::uint32_t);
    all_dtypes_tbl.addColumn("TheInt64", dt::int64_t);
    all_dtypes_tbl.addColumn("TheUInt64", dt::uint64_t);
    all_dtypes_tbl.addColumn("TheDouble", dt::double_t);
    all_dtypes_tbl.addColumn("TheString", dt::string_t);
    all_dtypes_tbl.addColumn("TheBlob", dt::blob_t);
    all_dtypes_tbl.setColumnDefaultValue("TheString", "HelloWorld");
    all_dtypes_tbl.setColumnDefaultValue("TheUInt64", 0xdeadbeef);
    all_dtypes_tbl.createIndexOn("TheInt64");
    all_dtypes_tbl.createCompoundIndexOn({"TheDouble", "TheString"});

    simdb::DatabaseManager db_mgr4("test5.db", true /*new db*/);
    EXPECT_NOTHROW(db_mgr4.appendSchema(schema4));

    simdb::DatabaseManager db_mgr5("test5.db", false /*connect to test5.db*/);
    EXPECT_TRUE(db_mgr5.getSchema() == db_mgr4.getSchema());

    // Create a record on the database with the reconstituted schema.
    // Start with the INSERT() method.
    auto all_dtypes_record = db_mgr4.INSERT(
        SQL_TABLE("AllDataTypes"),
        SQL_COLUMNS("TheInt32", "TheUInt32", "TheInt64", "TheUInt64", "TheDouble", "TheString", "TheBlob"),
        SQL_VALUES(INT32_MAX, UINT32_MAX, INT64_MAX, UINT64_MAX, 3.14, "blah", std::vector<int>{1,2,3}));

    EXPECT_EQUAL(all_dtypes_record->getPropertyInt32("TheInt32"), INT32_MAX);
    EXPECT_EQUAL(all_dtypes_record->getPropertyUInt32("TheUInt32"), UINT32_MAX);
    EXPECT_EQUAL(all_dtypes_record->getPropertyInt64("TheInt64"), INT64_MAX);
    EXPECT_EQUAL(all_dtypes_record->getPropertyUInt64("TheUInt64"), UINT64_MAX);
    EXPECT_EQUAL(all_dtypes_record->getPropertyDouble("TheDouble"), 3.14);
    EXPECT_EQUAL(all_dtypes_record->getPropertyString("TheString"), "blah");
    EXPECT_EQUAL(all_dtypes_record->getPropertyBlob<int>("TheBlob"), std::vector<int>({1,2,3}));

    REPORT_ERROR;
    return ERROR_CODE;
}
