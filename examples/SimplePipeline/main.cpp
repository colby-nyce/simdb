// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "SimplePipeline.hpp"

/// This example demonstrates a simple two-stage pipeline that compresses
/// data on one thread and writes it to a SQLite database on another thread.

REGISTER_SIMDB_APPLICATION(SimplePipeline);

TEST_INIT;

int main(int argc, char** argv)
{
    simdb::AppManagers app_mgrs;

    // Test misuse of getAppManager(db_file, create_if_needed)
    EXPECT_THROW(app_mgrs.getAppManager("test.db", false));

    // Test misuse of getAppManager() / getDatabaseManager() APIs
    EXPECT_THROW(app_mgrs.getAppManager());
    EXPECT_THROW(app_mgrs.getDatabaseManager());

    // Create the app/db managers
    auto& app_mgr = app_mgrs.getAppManager("test.db");
    auto& db_mgr = app_mgrs.getDatabaseManager();

    // Get coverage for getAppManager() overload
    EXPECT_EQUAL(&app_mgrs.getAppManager(), &app_mgr);

    // Get coverage for getDatabaseManager() method
    EXPECT_EQUAL(&app_mgrs.getDatabaseManager(), &db_mgr);

    // Setup...
    app_mgr.enableApp(SimplePipeline::NAME);
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
    app_mgrs.postSimLoopTeardown();

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

    // Get coverage for the getDatabaseManager(db_file) API
    EXPECT_EQUAL(&app_mgrs.getDatabaseManager("test.db"), &db_mgr);

    REPORT_ERROR;
    return ERROR_CODE;
}
