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
    meta_tbl.addColumn("NumInstsExecuted", dt::int64_t);

    EXPECT_TRUE(schema.hasTable("SimMetadata"));
    EXPECT_EQUAL(&schema.getTable("SimMetadata"), &meta_tbl);
    EXPECT_EQUAL(meta_tbl.getName(), "SimMetadata");
    EXPECT_TRUE(meta_tbl.hasColumn("NumInstsExecuted"));
    EXPECT_EQUAL(meta_tbl.getColumn("NumInstsExecuted").getName(), "NumInstsExecuted");
    EXPECT_EQUAL(meta_tbl.getColumn("NumInstsExecuted").getDataType(), dt::int64_t);

    // Negative tests for duplicates etc.
    EXPECT_THROW(schema.addTable("SimMetadata"));
    EXPECT_THROW(meta_tbl.addColumn("NumInstsExecuted", dt::int64_t));
    EXPECT_THROW(meta_tbl.getColumn("DOES_NOT_EXIST"));
    EXPECT_THROW(schema.getTable("DOES_NOT_EXIST"));

    // Test column default values
    auto& reports_tbl = schema.addTable("Reports");
    reports_tbl.addColumn("ReportName", dt::string_t);
    reports_tbl.addColumn("StartTick", dt::int64_t);
    reports_tbl.addColumn("EndTick", dt::int64_t);
    reports_tbl.addColumn("StatsValues", dt::blob_t);
    reports_tbl.setColumnDefaultValue("StartTick", 0);
    reports_tbl.setColumnDefaultValue("EndTick", -1);

    // Negative tests:
    // -- Bad column name
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("DOES_NOT_EXIST", 404));
    // -- Good column name, but data type mismatch
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("EndTick", "NOT_A_STRING"));
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("ReportName", 0xdeadbeef));
    EXPECT_THROW(reports_tbl.setColumnDefaultValue("EndTick", "NOT_A_STRING"));
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
    stat_ranges_tbl.addColumn("StartTick", dt::int64_t);
    stat_ranges_tbl.addColumn("EndTick", dt::int64_t);
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
    stat_ranges_tbl_dup.addColumn("StartTick", dt::int64_t);
    stat_ranges_tbl_dup.addColumn("EndTick", dt::int64_t);
    EXPECT_THROW(schema2.appendSchema(schema3));

    // Verify that all three schemas got past their negative tests
    // and can individually be used to instantiate databases.
    simdb::DatabaseManager db_mgr("test1.db", true);
    EXPECT_TRUE(db_mgr.appendSchema(schema));

    simdb::DatabaseManager db_mgr2("test2.db", true);
    EXPECT_TRUE(db_mgr2.appendSchema(schema2));

    simdb::DatabaseManager db_mgr3("test3.db", true);
    EXPECT_TRUE(db_mgr3.appendSchema(schema3));

    REPORT_ERROR;
    return ERROR_CODE;
}
