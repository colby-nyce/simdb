// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "SimplePipeline.hpp"

/// This example demonstrates the use of concurrently-running SimDB apps.
/// We will create four instances of the SimplePipeline app.

REGISTER_SIMDB_APPLICATION(SimplePipeline);

TEST_INIT;

int main()
{
    simdb::AppManagers app_mgrs;
    auto& app_mgr = app_mgrs.getAppManager("test.db");
    auto& db_mgr = app_mgrs.getDatabaseManager();

    // Disable pipeline messages; it clutters up stdout with so many running apps
    app_mgr.disableMessageLog();
    app_mgr.disableErrorLog();

    // Create 4 instances of the SimplePipeline app
    app_mgr.enableApp(SimplePipeline::NAME, 4);

    // Create all the apps
    app_mgr.createEnabledApps();

    // Should not be able to get app with unspecified instance
    EXPECT_THROW(app_mgr.getApp<SimplePipeline>());

    auto app1 = app_mgr.getAppInstance<SimplePipeline>(0);
    auto app2 = app_mgr.getAppInstance<SimplePipeline>(1);
    auto app3 = app_mgr.getAppInstance<SimplePipeline>(2);
    auto app4 = app_mgr.getAppInstance<SimplePipeline>(3);
    SimplePipeline* apps[] = {app1, app2, app3, app4};

    // Instantiate the DB schema
    app_mgr.createSchemas();

    // Initialize all pipelines
    app_mgr.initializePipelines();

    // Finally, launch all threads for all app pipelines
    app_mgr.openPipelines();

    // Simulate...
    std::map<size_t, std::vector<std::vector<double>>> test_data;
    for (size_t i = 0; i < 10000; ++i)
    {
        auto data = std::vector<double>(1000);
        for (auto& val : data)
        {
            val = rand() % 100 * M_PI;
        }

        auto instance = rand() % 4;
        test_data[instance].push_back(data);
        apps[instance]->process(data);
    }

    // Let the threads keep running for a second just to get
    // extra testing for all the running apps. This is really
    // just a sanity check of sorts.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Finish...
    app_mgr.postSimLoopTeardown();

    // Validate...
    for (size_t instance = 1; instance <= 4; ++instance)
    {
        auto query = db_mgr.createQuery("CompressedData");
        query->addConstraintForInt("AppInstance", simdb::Constraints::EQUAL, (int)instance);

        std::vector<char> blob;
        query->select("DataBlob", blob);

        const auto& expected_data = test_data[instance];
        size_t row_idx = 0;

        auto results = query->getResultSet();
        while (results.getNextRecord())
        {
            std::vector<double> actual_data;
            simdb::decompressData(blob, actual_data);
            EXPECT_EQUAL(actual_data, expected_data[row_idx]);
            ++row_idx;
        }
    }

    REPORT_ERROR;
    return ERROR_CODE;
}
