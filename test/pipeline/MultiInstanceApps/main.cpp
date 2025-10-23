// clang-format off

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppRegistration.hpp"
#include "simdb/pipeline/PipelineManager.hpp"
#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"
#include "SimDBTester.hpp"

class SimpleApp : public simdb::App
{
public:
    static constexpr auto NAME = "simple-app";

    SimpleApp(simdb::DatabaseManager* db_mgr, const std::string& desc)
        : db_mgr_(db_mgr)
        , description_(desc)
    {}

    ~SimpleApp() noexcept = default;

    std::string getDescription() const
    {
        return description_;
    }

    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& tbl = schema.addTable("SimpleTable");
        tbl.addColumn("IntValue", dt::int32_t);
        tbl.addColumn("Instance", dt::int32_t);
    }

    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        pipeline_mgr->createPipeline(NAME + std::to_string(getInstance()));
        auto db_accessor = pipeline_mgr->getAsyncDatabaseAccessor();

        auto db_task = db_accessor->createAsyncWriter<SimpleApp, int, void>(
            [this](int&& val,
                   simdb::pipeline::AppPreparedINSERTs* tables,
                   bool /*force*/)
            {
                auto inserter = tables->getPreparedINSERT("SimpleTable");
                inserter->setColumnValue(0, val);
                inserter->setColumnValue(1, (int)getInstance());
                inserter->createRecord();
                return simdb::pipeline::RunnableOutcome::DID_WORK;
            });

        pipeline_head_ = db_task->getTypedInputQueue<int>();
    }

    void process(int val)
    {
        pipeline_head_->emplace(val);
    }

private:
    simdb::DatabaseManager* db_mgr_ = nullptr;
    simdb::ConcurrentQueue<int>* pipeline_head_ = nullptr;
    std::string description_;
};

REGISTER_SIMDB_APPLICATION(SimpleApp);

namespace simdb
{

template <>
class AppFactory<SimpleApp> : public AppFactoryBase
{
public:
    using AppT = SimpleApp;

    void setDescription(size_t instance, const std::string& description)
    {
        instance_descriptions_[instance] = description;
    }

    AppT* createApp(DatabaseManager* db_mgr, size_t instance_num = 0) override
    {
        auto it = instance_descriptions_.find(instance_num);
        if (it == instance_descriptions_.end())
        {
            throw DBException("No description set for instance ") << instance_num;
        }
        return new AppT(db_mgr, it->second);
    }

    void defineSchema(Schema& schema) const override
    {
        AppT::defineSchema(schema);
    }

private:
    std::map<size_t, std::string> instance_descriptions_;
};

} // namespace simdb

int main(int argc, char** argv)
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    // Create 4 instances
    app_mgr.enableApp(SimpleApp::NAME, 4);

    auto factory = app_mgr.getAppFactory<SimpleApp>();
    factory->setDescription(1, "Foo");
    factory->setDescription(2, "Bar");
    factory->setDescription(3, "Fiz");
    factory->setDescription(4, "Buz");
    app_mgr.createEnabledApps();

    auto app1 = app_mgr.getApp<SimpleApp>(1);
    auto app2 = app_mgr.getApp<SimpleApp>(2);
    auto app3 = app_mgr.getApp<SimpleApp>(3);
    auto app4 = app_mgr.getApp<SimpleApp>(4);

    EXPECT_EQUAL(app1->getDescription(), "Foo");
    EXPECT_EQUAL(app2->getDescription(), "Bar");
    EXPECT_EQUAL(app3->getDescription(), "Fiz");
    EXPECT_EQUAL(app4->getDescription(), "Buz");

    app_mgr.createSchemas();
    app_mgr.postInit(argc, argv);
    app_mgr.openPipelines();

    // Should not be able to get app with unspecified instance
    EXPECT_THROW(app_mgr.getApp<SimpleApp>());

    // Simulate
    std::map<size_t, size_t> expected_val_counts = {
        {1, 0},
        {2, 0},
        {3, 0},
        {4, 0}
    };

    for (size_t tick = 1; tick <= 1000; ++tick)
    {
        auto instance = rand() % 4 + 1;
        auto app = app_mgr.getApp<SimpleApp>(instance);
        auto val = rand() % 1000;
        app->process(val);
        expected_val_counts[instance]++;
    }

    // Finish
    app_mgr.postSimLoopTeardown();

    // Validate
    auto query = db_mgr.createQuery("SimpleTable");
    for (const auto& [instance, expected_count] : expected_val_counts)
    {
        query->resetConstraints();
        query->addConstraintForInt("Instance", simdb::Constraints::EQUAL, (int)instance);
        EXPECT_EQUAL(query->count(), expected_count);
    }

    // This MUST be put at the end of unit test files' main() function.
    REPORT_ERROR;
    return ERROR_CODE;
}
