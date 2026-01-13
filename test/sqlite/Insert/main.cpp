// clang-format off

#include "SimDBTester.hpp"
#include "TestData.hpp"
#include "TestSchema.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

/// This test covers basic INSERT functionality for SimDB.

int main()
{
    simdb::Schema schema;
    test::utils::defineTestSchema(schema);

    simdb::DatabaseManager db_mgr("test.db", true);
    EXPECT_TRUE(db_mgr.appendSchema(schema));

    // Verify INSERT for integer types
    auto record1 = db_mgr.INSERT(
        SQL_TABLE("SignedIntegerTypes"),
        SQL_COLUMNS("SomeInt32", "SomeInt64"),
        SQL_VALUES(TEST_INT32, TEST_INT64));

    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt32"), TEST_INT32);
    EXPECT_EQUAL(record1->getPropertyInt64("SomeInt64"), TEST_INT64);

    record1->setPropertyInt32("SomeInt32", TEST_INT32 / 2);
    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt32"), TEST_INT32 / 2);

    record1->setPropertyInt64("SomeInt64", TEST_INT64 / 2);
    EXPECT_EQUAL(record1->getPropertyInt64("SomeInt64"), TEST_INT64 / 2);

    // Verify INSERT for floating-point types
    auto record2 = db_mgr.INSERT(
        SQL_TABLE("FloatingPointTypes"),
        SQL_COLUMNS("SomeDouble"),
        SQL_VALUES(TEST_DOUBLE));

    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE);

    record2->setPropertyDouble("SomeDouble", TEST_DOUBLE / 2);
    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE / 2);

    // Verify INSERT for string types
    auto record3 = db_mgr.INSERT(
        SQL_TABLE("StringTypes"),
        SQL_COLUMNS("SomeString"),
        SQL_VALUES(TEST_STRING));

    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING);

    record3->setPropertyString("SomeString", TEST_STRING + "2");
    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING + "2");

    // Verify INSERT for blob types
    auto record4 = db_mgr.INSERT(
        SQL_TABLE("BlobTypes"),
        SQL_COLUMNS("SomeBlob"),
        SQL_VALUES(TEST_VECTOR));

    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);

    record4->setPropertyBlob("SomeBlob", TEST_VECTOR2);
    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR2);

    auto record5 = db_mgr.INSERT(
        SQL_TABLE("BlobTypes"),
        SQL_COLUMNS("SomeBlob"),
        SQL_VALUES(TEST_BLOB));

    EXPECT_EQUAL(record5->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);

    record5->setPropertyBlob("SomeBlob", TEST_BLOB2.data_ptr, TEST_BLOB2.num_bytes);
    EXPECT_EQUAL(record5->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR2);

    simdb::SqlBlob blob(TEST_VECTOR);
    {
        auto record6 = db_mgr.INSERT(
            SQL_TABLE("BlobTypes"),
            SQL_COLUMNS("SomeBlob"),
            SQL_VALUES(blob));

        EXPECT_EQUAL(record6->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);
    }

    // Verify that bug is fixed: SQL_VALUES(..., <blob column>, ...)
    // would not compile when a blob (or vector) value was used in the
    // middle the SQL_VALUES (or anywhere but the last supplied value).
    db_mgr.INSERT(
        SQL_TABLE("MixAndMatch"),
        SQL_COLUMNS("SomeBlob", "SomeString"),
        SQL_VALUES(TEST_VECTOR, "foo"));

    db_mgr.INSERT(
        SQL_TABLE("MixAndMatch"),
        SQL_COLUMNS("SomeInt32", "SomeBlob", "SomeString"),
        SQL_VALUES(10, TEST_BLOB, "foo"));

    // Verify setDefaultValue()
    auto record6 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    EXPECT_EQUAL(record6->getPropertyInt32("DefaultInt32"), TEST_INT32);
    EXPECT_EQUAL(record6->getPropertyInt64("DefaultInt64"), TEST_INT64);
    EXPECT_EQUAL(record6->getPropertyString("DefaultString"), TEST_STRING);
    EXPECT_WITHIN_EPSILON(record6->getPropertyDouble("DefaultDouble"), TEST_DOUBLE);

    // Verify the PreparedINSERT class
    simdb::Schema schema2;
    using dt = simdb::SqlDataType;

    auto& high_volume_data_tbl = schema2.addTable("HighVolumeBlobs");
    high_volume_data_tbl.addColumn("StartTick", dt::uint32_t);
    high_volume_data_tbl.addColumn("EndTick", dt::uint32_t);
    high_volume_data_tbl.addColumn("DataBlob", dt::blob_t);
    db_mgr.appendSchema(schema2);

    auto high_volume_insert = db_mgr.prepareINSERT(
        SQL_TABLE("HighVolumeBlobs"),
        SQL_COLUMNS("StartTick", "EndTick", "DataBlob"));

    auto data_vec = TEST_VECTOR;
    high_volume_insert->setColumnValue(0, 100u);
    high_volume_insert->setColumnValue(1, UINT32_MAX / 2);
    high_volume_insert->setColumnValue(2, data_vec);

    auto high_volume_record_id1 = high_volume_insert->createRecord();

    data_vec.front() = 123;
    data_vec.back() = 456;
    high_volume_insert->setColumnValue(0, UINT32_MAX / 2 + 1);
    high_volume_insert->setColumnValue(1, UINT32_MAX);
    high_volume_insert->setColumnValue(2, data_vec);

    auto high_volume_record_id2 = high_volume_insert->createRecord();

    auto high_volume_record1 = db_mgr.findRecord("HighVolumeBlobs", high_volume_record_id1);
    EXPECT_EQUAL(high_volume_record1->getPropertyUInt32("StartTick"), 100);
    EXPECT_EQUAL(high_volume_record1->getPropertyUInt32("EndTick"), UINT32_MAX / 2);
    EXPECT_EQUAL(high_volume_record1->getPropertyBlob<int>("DataBlob"), TEST_VECTOR);

    auto high_volume_record2 = db_mgr.findRecord("HighVolumeBlobs", high_volume_record_id2);
    EXPECT_EQUAL(high_volume_record2->getPropertyUInt32("StartTick"), UINT32_MAX / 2 + 1);
    EXPECT_EQUAL(high_volume_record2->getPropertyUInt32("EndTick"), UINT32_MAX);
    EXPECT_EQUAL(high_volume_record2->getPropertyBlob<int>("DataBlob"), data_vec);

    REPORT_ERROR;
    return ERROR_CODE;
}
