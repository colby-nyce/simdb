#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>

#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb
{

class DatabaseManager;

class App
{
public:
    virtual ~App() = default;
    virtual void appendSchema() {}
    virtual void preInit(int argc, char** argv) { (void)argc; (void)argv; }
    virtual void preSim() {}
    virtual void postSim() {}
    virtual void teardown() {}

protected:
    int getAppID_() const { return app_id_; }

    App *const __this__ = this;

private:
    void setAppID_(int app_id) { app_id_ = app_id; }
    int app_id_ = 0;

    // Allow AppManager to set the app ID
    friend class AppManager;
};

class AppFactoryBase
{
public:
    virtual ~AppFactoryBase() = default;
    virtual App* createApp(DatabaseManager* db_mgr) const = 0;
};

template <typename AppT>
class AppFactory : public AppFactoryBase
{
public:
    App* createApp(DatabaseManager* db_mgr) const override
    {
        return new AppT(db_mgr);
    }
};

class AppManager
{
public:
    static AppManager& getInstance()
    {
        static AppManager instance;
        return instance;
    }

    template <typename AppT>
    void registerApp()
    {
        app_factories_[AppT::NAME] = std::make_unique<AppFactory<AppT>>();
    }

    void enableApp(const std::string& app_name)
    {
        if (!enabled_apps_.insert(app_name).second)
        {
            throw DBException("App already enabled: ") << app_name;
        }
    }

    bool enabled(const std::string& app_name) const
    {
        return enabled_apps_.find(app_name) != enabled_apps_.end();
    }

    bool instantiated(const std::string& app_name) const
    {
        return apps_.find(app_name) != apps_.end();
    }

    bool createEnabledApps(DatabaseManager* db_mgr)
    {
        for (const auto& app_name : enabled_apps_)
        {
            App* app = app_factories_[app_name]->createApp(db_mgr);
            apps_[app_name] = std::unique_ptr<App>(app);
        }

        return !apps_.empty();
    }

    template <typename AppT>
    AppT* getApp(bool must_exist = true)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");

        const auto app_name = AppT::NAME;
        auto it = apps_.find(app_name);
        if (it != apps_.end())
        {
            auto app = dynamic_cast<AppT*>(it->second.get());
            if (!app && must_exist)
            {
                throw DBException("App of type ") << app_name << " is not of type " << typeid(AppT).name();
            }
            return app;
        }
        else if (must_exist)
        {
            throw DBException("App not found: ") << app_name;
        }

        return nullptr;
    }

    void createSchemas(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                Schema schema;
                using dt = SqlDataType;

                auto& tbl = schema.addTable("RegisteredApps");
                tbl.addColumn("AppName", dt::string_t);
                db_mgr->appendSchema(schema);

                for (const auto& [name, app] : apps_)
                {
                    auto record = db_mgr->INSERT(
                        SQL_TABLE("RegisteredApps"),
                        SQL_COLUMNS("AppName"),
                        SQL_VALUES(name));

                    app->setAppID_(record->getId());
                    app->appendSchema();
                }
                return true;
            });
    }

    void preInit(DatabaseManager* db_mgr, int argc, char** argv)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->preInit(argc, argv);
                }
                return true;
            });
    }

    void preSim(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->preSim();
                }
                return true;
            });
    }

    void postSim(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->postSim();
                }
                return true;
            });
    }

    void teardown(DatabaseManager* db_mgr)
    {
        for (const auto& [name, app] : apps_)
        {
            app->teardown();
        }
    }

    void deleteApps()
    {
        apps_.clear();
        enabled_apps_.clear();
        app_factories_.clear();
    }

private:
    AppManager() = default;
    std::map<std::string, std::unique_ptr<AppFactoryBase>> app_factories_;
    std::map<std::string, std::unique_ptr<App>> apps_;
    std::set<std::string> enabled_apps_;
};

} // namespace simdb

template <typename AppT>
struct AppRegistration
{
    AppRegistration()
    {
        simdb::AppManager::getInstance().registerApp<AppT>();
    }
};

/// Use this macro in any translation unit to register an application with SimDB.
/// This should typically be done at file scope to ensure your app factory is
/// registered before much else has happened.
///
///   MyApp.hpp:
///   ----------------------------------------
///   class MyApp : public simdb::App
///   { ... };
///
///   MyApp.cpp:
///   ----------------------------------------
///   REGISTER_SIMDB_APPLICATION(MyApp);
///
#define REGISTER_SIMDB_APPLICATION(ApplicationType) \
    AppRegistration<ApplicationType> __##ApplicationType##_registration;
