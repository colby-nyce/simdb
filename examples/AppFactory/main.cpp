// clang-format off

#include "simdb/apps/AppRegistration.hpp"
#include "SimplePipeline.hpp"

// This example demonstrates how to create simdb::App subclasses
// using non-default factories. The reason to do this is if your
// app requires a constructor that is NOT what the default factory
// uses:
//
//     class MyApp : public simdb::App
//     {
//     public:
//         // Default factory assumes just DatabaseManager, so we
//         // need a different factory.
//         MyApp(simdb::DatabaseManager* db_mgr, int x, float y);
//     };

class MyApp : public SimplePipeline
{
public:
    static constexpr auto NAME = "my-app";

    MyApp(simdb::DatabaseManager* db_mgr, int x, float y)
        : SimplePipeline(db_mgr)
        , x_(x)
        , y_(y)
    {}

    static void defineSchema(simdb::Schema&)
    {
        // No schema for this app
    }

    int getX() const
    {
        return x_;
    }

    float getY() const
    {
        return y_;
    }

private:
    const int x_;
    const float y_;
};

// User-defined app factory goes in simdb namespace. Needs to be visible by the
// time you call REGISTER_SIMDB_APPLICATION.
namespace simdb
{
    template <>
    class AppFactory<MyApp> : public simdb::AppFactoryBase
    {
    public:
        using AppT = MyApp;

        void setCtorArgs(size_t instance_num, int x, float y)
        {
            ctor_args_by_instance_[instance_num] = std::make_pair(x, y);
        }

        AppT* createApp(DatabaseManager* db_mgr, size_t instance_num = 0) override
        {
            auto it = ctor_args_by_instance_.find(instance_num);
            if (it == ctor_args_by_instance_.end())
            {
                throw DBException("Invalid instance: ") << instance_num;
            }

            const auto [x, y] = it->second;
            return new AppT(db_mgr, x, y);
        }

        void defineSchema(Schema& schema) const override
        {
            AppT::defineSchema(schema);
        }

    private:
        using CtorArgs = std::pair<int, float>;
        std::map<size_t /*instance_num*/, CtorArgs> ctor_args_by_instance_;
    };
}

REGISTER_SIMDB_APPLICATION(MyApp);

TEST_INIT;

int main()
{
    simdb::DatabaseManager db_mgr("test.db", true);
    simdb::AppManager app_mgr(&db_mgr);

    // Enable 2 app instances
    app_mgr.enableApp(MyApp::NAME, 2);

    // Configure app constructor calls before createEnabledApps()
    const int x1 = 45;
    const float y1 = 7.77;
    app_mgr.getAppFactory<MyApp>()->setCtorArgs(1, x1, y1);

    const int x2 = 98;
    const float y2 = 3.14;
    app_mgr.getAppFactory<MyApp>()->setCtorArgs(2, x2, y2);

    // Create the apps
    app_mgr.createEnabledApps();

    // Validate
    auto app1 = app_mgr.getApp<MyApp>(1);
    auto app2 = app_mgr.getApp<MyApp>(2);
    EXPECT_EQUAL(app1->getX(), x1);
    EXPECT_EQUAL(app1->getY(), y1);
    EXPECT_EQUAL(app2->getX(), x2);
    EXPECT_EQUAL(app2->getY(), y2);

    REPORT_ERROR;
    return ERROR_CODE;
}
