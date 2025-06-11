/*
 * Tests for SQLite connections, INSERT, UPDATE, etc.
 *
 * This test is only for the core SQLite functionality of SimDB.
 * It does not test SimDB threads (async writes), compression
 * pipelines, or data collection.
 */

// clang-format off

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

static constexpr auto TEST_INT32 = std::numeric_limits<int32_t>::max();
static constexpr auto TEST_INT64 = std::numeric_limits<int64_t>::max();
static constexpr auto TEST_DOUBLE = std::numeric_limits<double>::max();
static constexpr auto TEST_EPSILON = std::numeric_limits<double>::epsilon();
static constexpr auto TEST_DOUBLE_MIN = std::numeric_limits<double>::min();
static constexpr auto TEST_DOUBLE_MAX = std::numeric_limits<double>::max();
static constexpr auto TEST_DOUBLE_PI = M_PI;
static constexpr auto TEST_DOUBLE_EXACT = 1.0;
static constexpr auto TEST_DOUBLE_INEXACT = (0.1 + 0.1 + 0.1);
static const std::string TEST_STRING = "TheExampleString";
static const std::vector<int> TEST_VECTOR = {1, 2, 3, 4, 5};
static const std::vector<int> TEST_VECTOR2 = {6, 7, 8, 9, 10};
static const simdb::SqlBlob TEST_BLOB = TEST_VECTOR;
static const simdb::SqlBlob TEST_BLOB2 = TEST_VECTOR2;

int main()
{
    DB_INIT;
    using dt = simdb::SqlDataType;

    simdb::Schema schema;

    schema.addTable("IntegerTypes")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeInt64", dt::int64_t);

    schema.addTable("FloatingPointTypes")
        .addColumn("SomeDouble", dt::double_t);

    schema.addTable("StringTypes")
        .addColumn("SomeString", dt::string_t);

    schema.addTable("BlobTypes")
        .addColumn("SomeBlob", dt::blob_t);

    schema.addTable("MixAndMatch")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeString", dt::string_t)
        .addColumn("SomeBlob", dt::blob_t);

    schema.addTable("DefaultValues")
        .addColumn("DefaultInt32", dt::int32_t)
        .addColumn("DefaultInt64", dt::int64_t)
        .addColumn("DefaultDouble", dt::double_t)
        .addColumn("DefaultString", dt::string_t)
        .setColumnDefaultValue("DefaultInt32", TEST_INT32)
        .setColumnDefaultValue("DefaultInt64", TEST_INT64)
        .setColumnDefaultValue("DefaultDouble", TEST_DOUBLE)
        .setColumnDefaultValue("DefaultString", TEST_STRING);

    schema.addTable("DefaultDoubles")
        .addColumn("DefaultEPS", dt::double_t)
        .addColumn("DefaultMIN", dt::double_t)
        .addColumn("DefaultMAX", dt::double_t)
        .addColumn("DefaultPI", dt::double_t)
        .addColumn("DefaultEXACT", dt::double_t)
        .addColumn("DefaultINEXACT", dt::double_t)
        .setColumnDefaultValue("DefaultEPS", TEST_EPSILON)
        .setColumnDefaultValue("DefaultMIN", TEST_DOUBLE_MIN)
        .setColumnDefaultValue("DefaultMAX", TEST_DOUBLE_MAX)
        .setColumnDefaultValue("DefaultPI", TEST_DOUBLE_PI)
        .setColumnDefaultValue("DefaultEXACT", TEST_DOUBLE_EXACT)
        .setColumnDefaultValue("DefaultINEXACT", TEST_DOUBLE_INEXACT);

    schema.addTable("IndexedColumns")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeDouble", dt::double_t)
        .addColumn("SomeString", dt::string_t)
        .createCompoundIndexOn({"SomeInt32", "SomeDouble", "SomeString"});

    schema.addTable("NonIndexedColumns")
        .addColumn("SomeInt32", dt::int32_t)
        .addColumn("SomeDouble", dt::double_t)
        .addColumn("SomeString", dt::string_t);

    simdb::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.appendSchema(schema));

    // Verify set/get APIs for integer types
    auto record1 = db_mgr.INSERT(SQL_TABLE("IntegerTypes"),
                                 SQL_COLUMNS("SomeInt32", "SomeInt64"),
                                 SQL_VALUES(TEST_INT32, TEST_INT64));

    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt32"), TEST_INT32);
    EXPECT_EQUAL(record1->getPropertyInt64("SomeInt64"), TEST_INT64);

    record1->setPropertyInt32("SomeInt32", TEST_INT32 / 2);
    EXPECT_EQUAL(record1->getPropertyInt32("SomeInt32"), TEST_INT32 / 2);

    record1->setPropertyInt64("SomeInt64", TEST_INT64 / 2);
    EXPECT_EQUAL(record1->getPropertyInt64("SomeInt64"), TEST_INT64 / 2);

    // Verify set/get APIs for floating-point types
    auto record2 = db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"),
                                 SQL_COLUMNS("SomeDouble"),
                                 SQL_VALUES(TEST_DOUBLE));

    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE);

    record2->setPropertyDouble("SomeDouble", TEST_DOUBLE / 2);
    EXPECT_EQUAL(record2->getPropertyDouble("SomeDouble"), TEST_DOUBLE / 2);

    // Verify set/get APIs for string types
    auto record3 = db_mgr.INSERT(SQL_TABLE("StringTypes"),
                                 SQL_COLUMNS("SomeString"),
                                 SQL_VALUES(TEST_STRING));

    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING);

    record3->setPropertyString("SomeString", TEST_STRING + "2");
    EXPECT_EQUAL(record3->getPropertyString("SomeString"), TEST_STRING + "2");

    // Verify set/get APIs for blob types
    auto record4 = db_mgr.INSERT(SQL_TABLE("BlobTypes"),
                                 SQL_COLUMNS("SomeBlob"),
                                 SQL_VALUES(TEST_VECTOR));

    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);

    record4->setPropertyBlob("SomeBlob", TEST_VECTOR2);
    EXPECT_EQUAL(record4->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR2);

    auto record5 = db_mgr.INSERT(SQL_TABLE("BlobTypes"),
                                 SQL_COLUMNS("SomeBlob"),
                                 SQL_VALUES(TEST_BLOB));

    EXPECT_EQUAL(record5->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);

    record5->setPropertyBlob("SomeBlob", TEST_BLOB2.data_ptr, TEST_BLOB2.num_bytes);
    EXPECT_EQUAL(record5->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR2);

    simdb::SqlBlob blob(TEST_VECTOR);
    {
        auto record6 = db_mgr.INSERT(SQL_TABLE("BlobTypes"),
                                    SQL_COLUMNS("SomeBlob"),
                                    SQL_VALUES(blob));

        EXPECT_EQUAL(record6->getPropertyBlob<int>("SomeBlob"), TEST_VECTOR);
    }

    // Verify that bug is fixed: SQL_VALUES(..., <blob column>, ...)
    // would not compile when a blob (or vector) value was used in the
    // middle the SQL_VALUES (or anywhere but the last supplied value).
    db_mgr.INSERT(SQL_TABLE("MixAndMatch"),
                  SQL_COLUMNS("SomeBlob", "SomeString"),
                  SQL_VALUES(TEST_VECTOR, "foo"));

    db_mgr.INSERT(SQL_TABLE("MixAndMatch"),
                  SQL_COLUMNS("SomeInt32", "SomeBlob", "SomeString"),
                  SQL_VALUES(10, TEST_BLOB, "foo"));

    // Verify setDefaultValue()
    auto record6 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    EXPECT_EQUAL(record6->getPropertyInt32("DefaultInt32"), TEST_INT32);
    EXPECT_EQUAL(record6->getPropertyInt64("DefaultInt64"), TEST_INT64);
    EXPECT_EQUAL(record6->getPropertyString("DefaultString"), TEST_STRING);
    EXPECT_WITHIN_EPSILON(record6->getPropertyDouble("DefaultDouble"), TEST_DOUBLE);

    // Verify findRecord() with bad ID.
    auto record7 = db_mgr.findRecord("DefaultValues", 404);
    EXPECT_EQUAL(record7.get(), nullptr);
    auto record8 = db_mgr.getRecord("DefaultValues", record6->getId());
    EXPECT_NOTEQUAL(record8.get(), nullptr);
    EXPECT_EQUAL(record8->getId(), record6->getId());

    // Verify record deletion.
    auto record9 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    auto record10 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    auto record11 = db_mgr.INSERT(SQL_TABLE("DefaultValues"));
    EXPECT_TRUE(record9->removeFromTable());
    EXPECT_FALSE(record9->removeFromTable());
    EXPECT_FALSE(db_mgr.removeRecordFromTable("DefaultValues", record9->getId()));
    EXPECT_TRUE(db_mgr.removeRecordFromTable("DefaultValues", record10->getId()));
    EXPECT_FALSE(db_mgr.removeRecordFromTable("DefaultValues", record10->getId()));
    EXPECT_NOTEQUAL(db_mgr.findRecord("DefaultValues", record11->getId()).get(), nullptr);
    EXPECT_NOTEQUAL(db_mgr.removeAllRecordsFromTable("DefaultValues"), 0);
    EXPECT_EQUAL(db_mgr.findRecord("DefaultValues", record11->getId()).get(), nullptr);
    EXPECT_NOTEQUAL(db_mgr.findRecord("StringTypes", record3->getId()).get(), nullptr);
    EXPECT_NOTEQUAL(db_mgr.removeAllRecordsFromAllTables(), 0);
    EXPECT_EQUAL(db_mgr.findRecord("StringTypes", record3->getId()).get(), nullptr);

    // To get ready for testing the SqlQuery class, first create some new records.
    //
    // IntegerTypes
    // ---------------------------------------------------------------------------------
    // SomeInt32    SomeInt64
    // 111          555
    // 222          555
    // 333          555
    // 111          777
    // 222          777
    // 333          101
    db_mgr.INSERT(SQL_TABLE("IntegerTypes"), SQL_COLUMNS("SomeInt32", "SomeInt64"), SQL_VALUES(111, 555));
    db_mgr.INSERT(SQL_TABLE("IntegerTypes"), SQL_COLUMNS("SomeInt32", "SomeInt64"), SQL_VALUES(222, 555));
    db_mgr.INSERT(SQL_TABLE("IntegerTypes"), SQL_COLUMNS("SomeInt32", "SomeInt64"), SQL_VALUES(333, 555));
    db_mgr.INSERT(SQL_TABLE("IntegerTypes"), SQL_COLUMNS("SomeInt32", "SomeInt64"), SQL_VALUES(111, 777));
    db_mgr.INSERT(SQL_TABLE("IntegerTypes"), SQL_COLUMNS("SomeInt32", "SomeInt64"), SQL_VALUES(222, 777));
    db_mgr.INSERT(SQL_TABLE("IntegerTypes"), SQL_COLUMNS("SomeInt32", "SomeInt64"), SQL_VALUES(333, 101));

    // FloatingPointTypes
    // ---------------------------------------------------------------------------------
    // SomeDouble
    // EPS
    // EPS
    // MIN
    // MIN
    // MAX
    // MAX
    // PI
    // PI
    // 1.0
    // 1.0
    // 0.3
    // 0.3
    for (auto val : {TEST_EPSILON, TEST_DOUBLE_MIN, TEST_DOUBLE_MAX, TEST_DOUBLE_PI, TEST_DOUBLE_EXACT, TEST_DOUBLE_INEXACT})
    {
        db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(val));
        db_mgr.INSERT(SQL_TABLE("FloatingPointTypes"), SQL_COLUMNS("SomeDouble"), SQL_VALUES(val));
    }

    // StringTypes
    // ---------------------------------------------------------------------------------
    // SomeString
    // foo
    // foo
    // bar
    // baz
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("foo"));
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("foo"));
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("bar"));
    db_mgr.INSERT(SQL_TABLE("StringTypes"), SQL_COLUMNS("SomeString"), SQL_VALUES("baz"));

    // MixAndMatch
    // ---------------------------------------------------------------------------------
    // SomeInt32    SomeString    SomeBlob
    // 10           foo           TEST_VECTOR
    // 10           bar           TEST_VECTOR
    // 20           foo           TEST_VECTOR2
    // 20           bar           TEST_VECTOR2
    db_mgr.INSERT(SQL_TABLE("MixAndMatch"),
                  SQL_COLUMNS("SomeInt32", "SomeString", "SomeBlob"),
                  SQL_VALUES(10, "foo", TEST_VECTOR));

    db_mgr.INSERT(SQL_TABLE("MixAndMatch"),
                  SQL_COLUMNS("SomeInt32", "SomeString", "SomeBlob"),
                  SQL_VALUES(10, "bar", TEST_VECTOR));

    db_mgr.INSERT(SQL_TABLE("MixAndMatch"),
                  SQL_COLUMNS("SomeInt32", "SomeString", "SomeBlob"),
                  SQL_VALUES(20, "foo", TEST_VECTOR2));

    db_mgr.INSERT(SQL_TABLE("MixAndMatch"),
                  SQL_COLUMNS("SomeInt32", "SomeString", "SomeBlob"),
                  SQL_VALUES(20, "bar", TEST_VECTOR2));

    // DefaultDoubles
    // ------------------------------------------------------------------------------------------------------
    // DefaultEPS    DefaultMIN       DefaultMAX       DefaultPI       DefaultEXACT       DefaultINEXACT
    // TEST_EPSILON  TEST_DOUBLE_MIN  TEST_DOUBLE_MAX  TEST_DOUBLE_PI  TEST_DOUBLE_EXACT  TEST_DOUBLE_INEXACT
    // TEST_EPSILON  TEST_DOUBLE_MIN  TEST_DOUBLE_MAX  TEST_DOUBLE_PI  TEST_DOUBLE_EXACT  TEST_DOUBLE_INEXACT
    db_mgr.INSERT(SQL_TABLE("DefaultDoubles"),
                  SQL_COLUMNS("DefaultEPS", "DefaultMIN", "DefaultMAX", "DefaultPI", "DefaultEXACT", "DefaultINEXACT"),
                  SQL_VALUES(TEST_EPSILON, TEST_DOUBLE_MIN, TEST_DOUBLE_MAX, TEST_DOUBLE_PI, TEST_DOUBLE_EXACT, TEST_DOUBLE_INEXACT));

    db_mgr.INSERT(SQL_TABLE("DefaultDoubles"),
                  SQL_COLUMNS("DefaultEPS", "DefaultMIN", "DefaultMAX", "DefaultPI", "DefaultEXACT", "DefaultINEXACT"),
                  SQL_VALUES(TEST_EPSILON, TEST_DOUBLE_MIN, TEST_DOUBLE_MAX, TEST_DOUBLE_PI, TEST_DOUBLE_EXACT, TEST_DOUBLE_INEXACT));

    db_mgr.safeTransaction(
        [&]()
        {
            // IndexedColumns
            // ------------------------------------------------------------------------------------------------------
            // SomeInt32    SomeDouble    SomeString
            // 1            1.1           "1.1"
            // 2            2.1           "2.1"
            // ...          ...           ...
            // 100000       100000.1      "100000.1"
            for (int idx = 1; idx <= 100000; ++idx)
            {
                auto val_int = idx;
                auto val_dbl = idx + 0.1;
                auto val_str = std::to_string(val_int);

                db_mgr.INSERT(SQL_TABLE("IndexedColumns"),
                              SQL_COLUMNS("SomeInt32", "SomeDouble", "SomeString"),
                              SQL_VALUES(val_int, val_dbl, val_str));
            }

            // NonIndexedColumns
            // ------------------------------------------------------------------------------------------------------
            // SomeInt32    SomeDouble    SomeString
            // 1            1.1           "1.1"
            // 2            2.1           "2.1"
            // ...          ...           ...
            // 100000       100000.1      "100000.1"
            for (int idx = 1; idx <= 100000; ++idx)
            {
                auto val_int = idx;
                auto val_dbl = idx + 0.1;
                auto val_str = std::to_string(val_int);

                db_mgr.INSERT(SQL_TABLE("NonIndexedColumns"),
                              SQL_COLUMNS("SomeInt32", "SomeDouble", "SomeString"),
                              SQL_VALUES(val_int, val_dbl, val_str));
            }
        });

    // Test SQL queries for integer types.
    int32_t i32;
    int64_t i64;

    auto query1 = db_mgr.createQuery("IntegerTypes");

    // Each successful call to result_set.getNextRecord() populates these variables.
    query1->select("SomeInt32", i32);
    query1->select("SomeInt64", i64);

    // SELECT COUNT(Id) should return 6 records.
    EXPECT_EQUAL(query1->count(), 6);
    {
        auto result_set = query1->getResultSet();

        // Iterate over the records one at a time and verify the data.
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 777);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 777);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 101);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());

        // Reset the iterator and make sure it can iterate again from the start.
        result_set.reset();
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_TRUE(result_set.getNextRecord());

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Add WHERE constraints, rerun the query, and check the results.
    query1->addConstraintForInt("SomeInt32", simdb::Constraints::NOT_EQUAL, 111);
    {
        auto result_set = query1->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 777);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 101);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    query1->addConstraintForInt("SomeInt64", simdb::Constraints::EQUAL, 777);
    {
        auto result_set = query1->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 777);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Remove WHERE constraints, add limit, rerun query.
    query1->resetConstraints();
    query1->setLimit(2);
    {
        auto result_set = query1->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 555);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Add ORDER BY clauses, rerun query.
    query1->resetLimit();
    query1->orderBy("SomeInt32", simdb::QueryOrder::DESC);
    query1->orderBy("SomeInt64", simdb::QueryOrder::ASC);
    {
        auto result_set = query1->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 101);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 777);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 777);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Test queries with NOT_EQUAL.
    query1->resetOrderBy();
    query1->addConstraintForInt("SomeInt32", simdb::Constraints::NOT_EQUAL, 222);
    {
        auto result_set = query1->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 111);
        EXPECT_EQUAL(i64, 777);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 333);
        EXPECT_EQUAL(i64, 101);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Test queries with NOT IN clause.
    query1->resetConstraints();
    query1->addConstraintForInt("SomeInt32", simdb::SetConstraints::NOT_IN_SET, {111, 333});
    {
        auto result_set = query1->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 555);

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 222);
        EXPECT_EQUAL(i64, 777);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Test SQL queries for floating-point types. This use case is special since it requires
    // a custom comparator to deal with machine precision issues.
    auto query2 = db_mgr.createQuery("FloatingPointTypes");

    // Each successful call to result_set.getNextRecord() populates these variables.
    double dbl;
    query2->select("SomeDouble", dbl);

    // SELECT COUNT(Id) should return 12 records.
    EXPECT_EQUAL(query2->count(), 12);
    {
        auto result_set = query2->getResultSet();

        // Iterate over the records one at a time and verify the data.
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_EPSILON);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_EPSILON);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_MIN);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_MIN);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_MAX);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_MAX);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_PI);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_PI);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_EXACT);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_EXACT);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_INEXACT);
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_WITHIN_EPSILON(dbl, TEST_DOUBLE_INEXACT);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Check WHERE clauses for doubles.
    for (auto target : {TEST_EPSILON, TEST_DOUBLE_MIN, TEST_DOUBLE_MAX, TEST_DOUBLE_PI, TEST_DOUBLE_EXACT, TEST_DOUBLE_INEXACT})
    {
        // Not using fuzzyMatch() - test for equality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::EQUAL, target, false);
        EXPECT_EQUAL(query2->count(), 2);

        // Using fuzzyMatch() - test for equality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::EQUAL, target, true);
        EXPECT_EQUAL(query2->count(), 2);

        // Not using fuzzyMatch() - test the IN clause, test for equality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::SetConstraints::IN_SET, {target}, false);
        EXPECT_EQUAL(query2->count(), 2);

        // Using fuzzyMatch() - test the IN clause, test for equality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::SetConstraints::IN_SET, {target}, true);
        EXPECT_EQUAL(query2->count(), 2);

        // Not using fuzzyMatch() - test for inequality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::NOT_EQUAL, target, false);
        EXPECT_EQUAL(query2->count(), 10);

        // Using fuzzyMatch() - test for inequality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::NOT_EQUAL, target, true);
        EXPECT_EQUAL(query2->count(), 10);

        // Not using fuzzyMatch() - test the IN clause, test for inequality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::SetConstraints::NOT_IN_SET, {target}, false);
        EXPECT_EQUAL(query2->count(), 10);

        // Using fuzzyMatch() - test the IN clause, test for inequality.
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::SetConstraints::NOT_IN_SET, {target}, true);
        EXPECT_EQUAL(query2->count(), 10);
    }

    // Check WHERE clause for comparisons using <, <=, >, >=
    for (auto fuzzy : {false, true})
    {
        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::LESS, TEST_DOUBLE_PI, fuzzy);
        EXPECT_EQUAL(query2->count(), 8);

        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::LESS_EQUAL, TEST_DOUBLE_PI, fuzzy);
        EXPECT_EQUAL(query2->count(), 10);

        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::GREATER, TEST_DOUBLE_PI, fuzzy);
        EXPECT_EQUAL(query2->count(), 2);

        query2->resetConstraints();
        query2->addConstraintForDouble("SomeDouble", simdb::Constraints::GREATER_EQUAL, TEST_DOUBLE_PI, fuzzy);
        EXPECT_EQUAL(query2->count(), 4);
    }

    // Test queries against double targets, with and without fuzzyMatch().
    auto query3 = db_mgr.createQuery("DefaultDoubles");

    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultEPS", simdb::Constraints::EQUAL, TEST_EPSILON, false);
    EXPECT_EQUAL(query3->count(), 2);
    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultEPS", simdb::Constraints::EQUAL, TEST_EPSILON, true);
    EXPECT_EQUAL(query3->count(), 2);

    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultMIN", simdb::Constraints::EQUAL, TEST_DOUBLE_MIN, false);
    EXPECT_EQUAL(query3->count(), 2);
    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultMIN", simdb::Constraints::EQUAL, TEST_DOUBLE_MIN, true);
    EXPECT_EQUAL(query3->count(), 2);

    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultMAX", simdb::Constraints::EQUAL, TEST_DOUBLE_MAX, false);
    EXPECT_EQUAL(query3->count(), 2);
    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultMAX", simdb::Constraints::EQUAL, TEST_DOUBLE_MAX, true);
    EXPECT_EQUAL(query3->count(), 2);

    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultPI", simdb::Constraints::EQUAL, TEST_DOUBLE_PI, false);
    EXPECT_EQUAL(query3->count(), 2);
    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultPI", simdb::Constraints::EQUAL, TEST_DOUBLE_PI, true);
    EXPECT_EQUAL(query3->count(), 2);

    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultEXACT", simdb::Constraints::EQUAL, TEST_DOUBLE_EXACT, false);
    EXPECT_EQUAL(query3->count(), 2);
    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultEXACT", simdb::Constraints::EQUAL, TEST_DOUBLE_EXACT, true);
    EXPECT_EQUAL(query3->count(), 2);

    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultINEXACT", simdb::Constraints::EQUAL, TEST_DOUBLE_INEXACT, false);
    EXPECT_EQUAL(query3->count(), 2);
    query3->resetConstraints();
    query3->addConstraintForDouble("DefaultINEXACT", simdb::Constraints::EQUAL, TEST_DOUBLE_INEXACT, true);
    EXPECT_EQUAL(query3->count(), 2);

    // Test SQL queries for string types.
    auto query4 = db_mgr.createQuery("StringTypes");

    // Each successful call to result_set.getNextRecord() populates these variables.
    std::string str;
    query4->select("SomeString", str);

    // SELECT COUNT(Id) should return 4 records.
    EXPECT_EQUAL(query4->count(), 4);
    {
        auto result_set = query4->getResultSet();

        // Iterate over the records one at a time and verify the data.
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "foo");
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "foo");
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "bar");
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "baz");

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Add WHERE constraints, rerun the query, and check the results.
    query4->addConstraintForString("SomeString", simdb::Constraints::EQUAL, "foo");
    {
        auto result_set = query4->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "foo");
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "foo");

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    query4->resetConstraints();
    query4->addConstraintForString("SomeString", simdb::SetConstraints::IN_SET, {"bar", "baz"});
    query4->orderBy("SomeString", simdb::QueryOrder::DESC);
    {
        auto result_set = query4->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "baz");
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(str, "bar");

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    // MixAndMatch
    // ---------------------------------------------------------------------------------
    // SomeInt32    SomeString    SomeBlob
    // 10           foo           TEST_VECTOR
    // 10           bar           TEST_VECTOR
    // 20           foo           TEST_VECTOR2
    // 20           bar           TEST_VECTOR2

    // Test queries that include multiple kinds of data type constraints,
    // and which includes a blob column.
    auto query5 = db_mgr.createQuery("MixAndMatch");

    // Each successful call to result_set.getNextRecord() populates these variables.
    std::vector<int> ivec;
    query5->select("SomeInt32", i32);
    query5->select("SomeString", str);
    query5->select("SomeBlob", ivec);

    // SELECT COUNT(Id) should return 4 records.
    EXPECT_EQUAL(query5->count(), 4);

    query5->addConstraintForInt("SomeInt32", simdb::Constraints::EQUAL, 20);
    query5->addConstraintForString("SomeString", simdb::Constraints::EQUAL, "foo");
    {
        auto result_set = query5->getResultSet();

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 20);
        EXPECT_EQUAL(str, "foo");
        EXPECT_EQUAL(ivec, TEST_VECTOR2);

        // We should have read all the records.
        EXPECT_FALSE(result_set.getNextRecord());
    }

    auto query7 = db_mgr.createQuery("NonIndexedColumns");

    // Make sure the tables have the same number of records first.
    int IndexedColumns_id, NonIndexedColumns_id;
    query7->select("Id", NonIndexedColumns_id);
    query7->addConstraintForInt("SomeInt32", simdb::Constraints::EQUAL, 100000);
    query7->addConstraintForDouble("SomeDouble", simdb::Constraints::EQUAL, 100000.1);
    query7->addConstraintForString("SomeString", simdb::Constraints::EQUAL, "100000");

    // Ensure that we can connect a new DatabaseManager to a .db that was
    // created by another DatabaseManager.
    simdb::DatabaseManager db_mgr2(db_mgr.getDatabaseFilePath());

    // Verify that we cannot alter the schema since this second DatabaseManager
    // did not instantiate the schema in the first place.
    simdb::Schema schema2;

    schema2.addTable("SomeTable")
        .addColumn("SomeColumn", dt::string_t);

    EXPECT_THROW(db_mgr2.appendSchema(schema2));

    int id;
    query7->select("Id", id);
    query7->select("SomeInt32", i32);
    query7->select("SomeDouble", dbl);
    query7->select("SomeString", str);

    {
        auto result_set = query7->getResultSet();
        EXPECT_TRUE(result_set.getNextRecord());
    }

    auto record12 = db_mgr2.findRecord("NonIndexedColumns", id);
    EXPECT_EQUAL(record12->getPropertyInt32("SomeInt32"), i32);
    EXPECT_EQUAL(record12->getPropertyDouble("SomeDouble"), dbl);
    EXPECT_EQUAL(record12->getPropertyString("SomeString"), str);

    // Ensure that we can append tables to an existing database schema.
    simdb::Schema schema3;

    schema3.addTable("AppendedTable").addColumn("SomeInt32", dt::int32_t);

    db_mgr.appendSchema(schema3);

    db_mgr.INSERT(SQL_TABLE("AppendedTable"), SQL_COLUMNS("SomeInt32"), SQL_VALUES(101));
    db_mgr.INSERT(SQL_TABLE("AppendedTable"), SQL_COLUMNS("SomeInt32"), SQL_VALUES(101));
    db_mgr.INSERT(SQL_TABLE("AppendedTable"), SQL_COLUMNS("SomeInt32"), SQL_VALUES(202));

    auto query8 = db_mgr.createQuery("AppendedTable");
    query8->addConstraintForInt("SomeInt32", simdb::Constraints::EQUAL, 101);
    EXPECT_EQUAL(query8->count(), 2);

    // Ensure that we can execute queries with OR clauses. Here we will test:
    //   SELECT COUNT(Id) FROM MixAndMatch WHERE (SomeInt32 = 10 AND SomeString = 'foo') OR (SomeString = 'foo')
    auto query9 = db_mgr.createQuery("MixAndMatch");

    query9->select("SomeInt32", i32);
    query9->select("SomeString", str);

    query9->addConstraintForInt("SomeInt32", simdb::Constraints::EQUAL, 10);
    query9->addConstraintForString("SomeString", simdb::Constraints::EQUAL, "foo");
    auto clause1 = query9->releaseConstraintClauses();

    query9->addConstraintForString("SomeString", simdb::Constraints::EQUAL, "foo");
    auto clause2 = query9->releaseConstraintClauses();

    query9->addCompoundConstraint(clause1, simdb::QueryOperator::OR, clause2);
    EXPECT_EQUAL(query9->count(), 2);

    {
        auto result_set = query9->getResultSet();
        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 10);
        EXPECT_EQUAL(str, "foo");

        EXPECT_TRUE(result_set.getNextRecord());
        EXPECT_EQUAL(i32, 20);
        EXPECT_EQUAL(str, "foo");

        EXPECT_FALSE(result_set.getNextRecord());
    }

    // Verify that we cannot open a database connection for an invalid file
    EXPECT_THROW(simdb::DatabaseManager db_mgr3(__FILE__));

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
