// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "SimplePipeline.hpp"

/// This example demonstrates a simple two-stage pipeline that compresses
/// data on one thread and writes it to a SQLite database on another thread.

REGISTER_SIMDB_APPLICATION(SimplePipeline);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);
    app_mgr.enableApp(SimplePipeline::NAME);

    // Setup...
    app_mgr.createEnabledApps();
    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.initializePipelines();
    app_mgr.openPipelines();

    // Simulate...
    auto app = app_mgr.getApp<SimplePipeline>();
    constexpr int NUM_VECS = 1000;
    constexpr int VEC_SIZE = 1000;
    std::vector<std::vector<double>> test_data;

    for (int i = 0; i < NUM_VECS; ++i) {
        std::vector<double> data(VEC_SIZE, static_cast<double>(i));
        test_data.push_back(data);
        app->process(data);
    }

    // Finalize...
    app_mgr.postSimLoopTeardown();

    // Validate...
    auto query = db_mgr.createQuery("CompressedData");

    std::vector<char> blob;
    query->select("DataBlob", blob);

    auto results = query->getResultSet();
    size_t row_idx = 0;
    while (results.getNextRecord())
    {
        std::vector<double> decompressed_data;
        simdb::decompressData(blob, decompressed_data);
        EXPECT_EQUAL(decompressed_data, test_data[row_idx]);
        row_idx++;
    }

    REPORT_ERROR;
    return ERROR_CODE;
}
