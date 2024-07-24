/*
 \brief Tests for SQLite connections, INSERT, UPDATE, etc.
 */

#include "simdb/test/SimDBTester.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

TEST_INIT;

int main()
{
    using dt = simdb::ColumnDataType;

    simdb::Schema schema;

    schema.addTable("Metadata")
        .addColumn("Name", dt::string_t)
        .addColumn("SomeInt", dt::int32_t)
        .addColumn("SomeDouble", dt::double_t)
        .addColumn("SomeString", dt::string_t)
        .addColumn("SomeBlob", dt::blob_t)
        .addColumn("DefaultInt", dt::int32_t)->setDefaultValue(4)
        .addColumn("DefaultDouble", dt::double_t)->setDefaultValue(3.14)
        .addColumn("DefaultString", dt::string_t)->setDefaultValue("foo");

    simdb::DatabaseManager db_mgr;
    EXPECT_TRUE(db_mgr.createDatabaseFromSchema(schema));

    // Create some records and verify the INSERT was successful.
    auto record1 = db_mgr.INSERT(SQL_TABLE("Metadata"), SQL_COLUMNS("SomeInt", "SomeDouble"), SQL_VALUES(777, 3.14));
    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt"), 777);
    EXPECT_EQUAL(record1->getPropertyDouble("SomeDouble"), 3.14);

    simdb::Blob blob;
    std::vector<int> input_vals{1,2,3,4,5};
    std::vector<int> output_vals;
    blob.data_ptr = input_vals.data();
    blob.num_bytes = input_vals.size() * sizeof(int);
    auto record2 = db_mgr.INSERT(SQL_TABLE("Metadata"), SQL_COLUMNS("SomeString", "SomeBlob"), SQL_VALUES("blah", blob));
    EXPECT_EQUAL(record2->getPropertyString("SomeString"), "blah");
    record2->getPropertyBlob("SomeBlob", output_vals);
    EXPECT_EQUAL(input_vals, output_vals);

    // Verify setDefaultValue()
    auto record3 = db_mgr.INSERT(SQL_TABLE("Metadata"));
    EXPECT_EQUAL(record3->getPropertyInt32("DefaultInt"), 4);
    EXPECT_EQUAL(record3->getPropertyDouble("DefaultDouble"), 3.14);
    EXPECT_EQUAL(record3->getPropertyString("DefaultString"), "foo");
}
