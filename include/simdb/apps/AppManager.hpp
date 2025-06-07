#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

#include <map>
#include <set>

namespace simdb
{

/// @brief Singleton class that manages all SimDB applications.
///
/// This class is responsible for registering, enabling, instantiating,
/// and managing the lifecycle of all SimDB applications running in a
/// simulation.
class AppManager
{
public:
    static AppManager& getInstance()
    {
        static AppManager instance;
        return instance;
    }

    /// Register an app as early as possible (doesn't create it yet) Register
    /// your app by adding this macro to a translation unit at file scope:
    ///
    ///   class MyApp : public simdb::App { ... };
    ///   REGISTER_SIMDB_APPLICATION(MyApp); 
    template <typename AppT>
    void registerApp()
    {
        app_factories_[AppT::NAME] = std::make_unique<AppFactory<AppT>>();
    }

    /// After parsing command line arguments or configuration files,
    /// enable an app by its name. This will allow the app to be instantiated
    /// and run during the simulation lifecycle.
    ///
    ///   class MyApp : public simdb::App
    ///   {
    ///   public:
    ///       static constexpr const char* NAME = "MyApp";
    ///       // ...
    ///   };
    ///
    ///   // Somewhere in your main function or config parsing code (meaning,
    ///   // after command line args are parsed and very early in the simulation):
    ///   simdb::AppManager::getInstance().enableApp(MyApp::NAME);
    void enableApp(const std::string& app_name)
    {
        if (!enabled_apps_.insert(app_name).second)
        {
            throw DBException("App already enabled: ") << app_name;
        }
    }

    /// Check if your app is enabled (might not be instantiated yet).
    bool enabled(const std::string& app_name) const
    {
        return enabled_apps_.find(app_name) != enabled_apps_.end();
    }

    /// Check if your app is instantiated (enabled and created).
    bool instantiated(const std::string& app_name) const
    {
        return apps_.find(app_name) != apps_.end();
    }

    /// Before creating the apps, you can override the number of compression
    /// threads for a specific app. Your app constructor takes this argument
    /// and gives it to its ThreadedSink ctor.
    ///
    /// Note that the default is 0. You can also call enableDefaultCompression()
    /// to set the number of compression threads for all apps to 1.
    void setNumCompressionThreads(const std::string& app_name, size_t num_threads)
    {
        auto it = app_factories_.find(app_name);
        if (it != app_factories_.end())
        {
            it->second->setNumCompressionThreads(num_threads);
        }
        else
        {
            throw DBException("App not found: ") << app_name;
        }
    }

    /// Enable default compression for all apps. This sets the number of compression
    /// threads to 1 for all registered apps, and must be called prior to instantiating
    /// the apps with createEnabledApps().
    void enableDefaultCompression()
    {
        for (auto& [name, factory] : app_factories_)
        {
            factory->setNumCompressionThreads(1);
        }
    }

    /// Call after command line args and config files are parsed.
    bool createEnabledApps(DatabaseManager* db_mgr)
    {
        for (const auto& app_name : enabled_apps_)
        {
            App* app = app_factories_[app_name]->createApp(db_mgr);
            apps_[app_name] = std::unique_ptr<App>(app);
        }

        return !apps_.empty();
    }

    /// Get an instantiated app. Throws if not found.
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

    /// Create app-specific schemas for the instantiated apps.
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

                    app->app_id_ = record->getId();
                    app->appendSchema();
                }
            });
    }

    /// Call this after command line args and config files are parsed.
    void preInit(DatabaseManager* db_mgr, int argc, char** argv)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->preInit(argc, argv);
                }
            });
    }

    /// Call this before the simulation loop starts, but after
    /// the simulator is fully initialized.
    void preSim(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->preSim();
                }
            });
    }

    /// Call this after the simulation loop ends for post-processing tasks.
    void postSim(DatabaseManager* db_mgr)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->postSim();
                }
            });
    }

    /// Call this after the simulation ends for resource cleanup tasks
    /// such as closing files, releasing memory, flushing/shutting down
    /// background threads, etc.
    void teardown(DatabaseManager* db_mgr)
    {
        for (const auto& [name, app] : apps_)
        {
            app->teardown();
        }
    }

    /// Delete all instantiated apps. This may be needed since AppManager
    /// is a singleton and your simulator might want to call app destructors
    /// before the AppManager itself is destroyed on program exit.
    void deleteApps()
    {
        apps_.clear();
        enabled_apps_.clear();
        app_factories_.clear();
    }

private:
    AppManager() = default;

    /// Registered app factories.
    std::map<std::string, std::unique_ptr<AppFactoryBase>> app_factories_;

    /// Instantiated apps (implicitly enabled).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    std::set<std::string> enabled_apps_;
};

} // namespace simdb
