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
    bool instantiated(const std::string& app_name, const DatabaseManager* db_mgr) const
    {
        return instantiated(app_name, db_mgr->getDatabaseFilePath());
    }

    /// Check if your app is instantiated (enabled and created).
    bool instantiated(const std::string& app_name, const std::string& db_file) const
    {
        const auto key = app_name + "_" + db_file;
        return apps_.find(key) != apps_.end();
    }

    /// Optionally reconfigure the pipeline mode for an app.
    void configureAppPipeline(const std::string& app_name, const DatabaseManager* db_mgr, AppPipelineMode mode)
    {
        configureAppPipeline(app_name, db_mgr->getDatabaseFilePath(), mode);
    }

    /// Optionally reconfigure the pipeline mode for an app.
    void configureAppPipeline(const std::string& app_name, const std::string& db_file, AppPipelineMode mode)
    {
        auto it = app_factories_.find(app_name);
        if (it == app_factories_.end())
        {
            throw DBException("App not registered: ") << app_name;
        }

        it->second->setPipelineMode(mode);
    }

    /// Finalize the app pipeline. Cannot be called twice. Must be called before createEnabledApps().
    void finalizeAppPipeline()
    {
        if (async_pipeline_)
        {
            throw DBException("App pipeline already finalized.");
        }

        bool async_compression_enabled = false;
        for (const auto& [name, factory] : app_factories_)
        {
            auto mode = factory->getPipelineMode();
            if (mode == AppPipelineMode::COMPRESS_SEPARATE_THREAD_THEN_WRITE_DB_THREAD)
            {
                async_compression_enabled = true;
                break;
            }
        }

        async_pipeline_ = std::make_unique<AsyncPipeline>(async_compression_enabled);
    }

    /// Call after command line args and config files are parsed.
    void createEnabledApps(DatabaseManager* db_mgr)
    {
        if (!async_pipeline_)
        {
            throw DBException("App pipeline must be finalized before creating enabled apps.");
        }

        for (const auto& app_name : enabled_apps_)
        {
            App* app = app_factories_[app_name]->createApp(db_mgr, *async_pipeline_);
            const auto key = app_name + "_" + db_mgr->getDatabaseFilePath();
            apps_[key] = std::unique_ptr<App>(app);
        }
    }

    /// Get an instantiated app. Throws if not found.
    template <typename AppT>
    AppT* getApp(DatabaseManager* db_mgr, bool must_exist = true)
    {
        return getApp<AppT>(db_mgr->getDatabaseFilePath(), must_exist);
    }

    /// Get an instantiated app. Throws if not found.
    template <typename AppT>
    AppT* getApp(const std::string& db_file, bool must_exist = true)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");

        const auto app_name = AppT::NAME;
        const auto key = app_name + ("_" + db_file);
        auto it = apps_.find(key);
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

                for (const auto& [key, app] : apps_)
                {
                    // The key is formatted as "<AppName>_<db_file>"
                    auto pos = key.find("_" + db_mgr->getDatabaseFilePath());
                    if (pos == std::string::npos)
                    {
                        throw DBException("App key does not match database file: ") << key;
                    }

                    auto app_name = key.substr(0, pos);

                    // Ensure no duplicates
                    auto query = db_mgr->createQuery("RegisteredApps");
                    query->addConstraintForString("AppName", Constraints::EQUAL, app_name);
                    if (query->count() > 0)
                    {
                        throw DBException("App already registered: ") << app_name;
                    }

                    auto record = db_mgr->INSERT(
                        SQL_TABLE("RegisteredApps"),
                        SQL_COLUMNS("AppName"),
                        SQL_VALUES(app_name));

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
    ///
    /// Optionally pass in the DatabaseManager if you want to only delete
    /// those apps that are associated with that specific database. Else
    /// all apps will be deleted.
    void deleteApps(DatabaseManager* db_mgr = nullptr)
    {
        if (db_mgr)
        {
            const auto s = "_" + db_mgr->getDatabaseFilePath();
            std::vector<std::string> delete_keys;
            for (const auto& [key, app] : apps_)
            {
                if (key.find(s) != std::string::npos)
                {
                    delete_keys.push_back(key);
                }
            }

            for (const auto& key : delete_keys)
            {
                apps_.erase(key);
            }
        }
        else
        {
            apps_.clear();
        }
    }

private:
    AppManager() = default;

    /// Registered app factories.
    std::map<std::string, std::unique_ptr<AppFactoryBase>> app_factories_;

    /// Instantiated apps (implicitly enabled).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    std::set<std::string> enabled_apps_;

    /// One AsyncPipeline for all apps to share, even if they write to different databases.
    std::unique_ptr<AsyncPipeline> async_pipeline_;
};

} // namespace simdb
