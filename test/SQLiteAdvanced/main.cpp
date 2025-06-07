/*
 * These tests show how to use SimDB's more advanced features:
 *   - Minifying std::string with TinyStrings
 *   - Building async simulation->compression->database pipelines
 */

// clang-format off

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/TinyStrings.hpp"
#include "simdb/serialize/ThreadedSink.hpp"
#include "simdb/schema/Blob.hpp"
#include "simdb/test/SimDBTester.hpp"

TEST_INIT;

// TinyStrings is a utility that allows you to minimize the size of
// std::string columns in your database by mapping them to unique IDs.
// The mappings are held in the "TinyStringIDs" table.
void TestTinyStrings()
{
    DB_INIT;

    using dt = simdb::SqlDataType;
    simdb::Schema schema;

    // Create the database.
    simdb::DatabaseManager db_mgr("test.db");

    // Associate the database with the TinyStrings object and tell it which table
    // to use for storing the string IDs (defaults to "TinyStringIDs").
    simdb::TinyStrings tiny_strings(&db_mgr);

    // Insert some strings into the TinyStrings map and serialize them to the database.
    std::vector<std::string> strings = {
        "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"
    };

    for (const auto& str : strings)
    {
        auto string_id = tiny_strings[str];
        (void)string_id;
    }

    tiny_strings.serialize();

    // Add the same strings to the TinyStrings map and serialize again.
    for (const auto& str : strings)
    {
        auto string_id = tiny_strings[str];
        (void)string_id;
    }

    tiny_strings.serialize();

    // Query the database. Ensure the word "the" only appears once.
    auto query = db_mgr.createQuery("TinyStringIDs");
    query->addConstraintForString("StringValue", simdb::Constraints::EQUAL, "the");
    EXPECT_EQUAL(query->count(), 1);
}

/// TestDatabasePipeline demonstrates how to build a database pipeline for async
/// processing of data, with optional compression performed across a configurable
/// number of threads.
void TestDatabasePipeline(size_t compression_threads)
{
    DB_INIT;

    using dt = simdb::SqlDataType;
    simdb::Schema schema;

    // Create a table to hold some data blobs.
    auto& data_blob_tbl = schema.addTable("DataBlobs");
    data_blob_tbl.addColumn("Tick", dt::int64_t);
    data_blob_tbl.addColumn("DataBlob", dt::blob_t);
    data_blob_tbl.addColumn("IsCompressed", dt::int32_t);

    // Create the database.
    simdb::DatabaseManager db_mgr("test.db");
    EXPECT_TRUE(db_mgr.appendSchema(schema));

    // End-of-pipeline callback to write the data to the database.
    auto end_of_pipeline_callback = [](simdb::DatabaseManager* db_mgr, simdb::DatabaseEntry&& entry)
    {
        simdb::SqlBlob blob;
        blob.data_ptr = entry.data_ptr;
        blob.num_bytes = entry.num_bytes;

        db_mgr->INSERT(
            SQL_TABLE("DataBlobs"),
            SQL_COLUMNS("Tick", "DataBlob", "IsCompressed"),
            SQL_VALUES(entry.tick, blob, entry.compressed ? 1 : 0));
    };

    // Create a ThreadedSink to build the pipeline.
    simdb::ThreadedSink sink(&db_mgr, end_of_pipeline_callback, compression_threads);

    // Send a blob down the pipeline.
    std::vector<char> alphabet;
    for (int letter = 'a'; letter <= 'z'; ++letter)
    {
        alphabet.push_back(static_cast<char>(letter));
    }

    simdb::DatabaseEntry entry;
    entry.tick = 12345;
    entry.data_ptr = alphabet.data();
    entry.num_bytes = alphabet.size();
    entry.container = alphabet;
    sink.push(std::move(entry));

    // Let the pipeline finish processing.
    sink.teardown();

    // Query the data blob and verify.
    auto query = db_mgr.createQuery("DataBlobs");
    query->addConstraintForInt("Tick", simdb::Constraints::EQUAL, 12345);

    int is_compressed;
    query->select("IsCompressed", is_compressed);

    std::vector<char> data_blob;
    query->select("DataBlob", data_blob);

    auto result_set = query->getResultSet();
    EXPECT_TRUE(result_set.getNextRecord());

    if (is_compressed)
    {
        std::vector<char> decompressed_data;
        simdb::decompressDataVec(data_blob, decompressed_data);
        std::swap(data_blob, decompressed_data);
    }

    EXPECT_EQUAL(data_blob, alphabet);
}

int main()
{
    TestTinyStrings();       // Test string minification.
    TestDatabasePipeline(0); // Test pipeline (no compression, just async DB writes).
    TestDatabasePipeline(1); // Test pipeline (one compression thread and async DB writes).

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
