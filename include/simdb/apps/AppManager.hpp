#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/apps/PipelineApp.hpp"
#include "simdb/pipeline/Pipeline.hpp"
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
    /// Register an app as early as possible (doesn't create it yet) Register
    /// your app by adding this macro to a translation unit at file scope:
    ///
    ///   class MyApp : public simdb::App { ... };
    ///   REGISTER_SIMDB_APPLICATION(MyApp); 
    template <typename AppT>
    static void registerApp()
    {
        auto it = app_factories_.find(AppT::NAME);
        if (it != app_factories_.end())
        {
            throw DBException("App already registered: ") << AppT::NAME;
        }
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
        enabled_apps_.insert(app_name);
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

    /// Call after command line args and config files are parsed.
    void createEnabledApps(DatabaseManager* db_mgr)
    {
        for (const auto& app_name : enabled_apps_)
        {
            App* app = app_factories_[app_name]->createApp(db_mgr);
            const auto key = app_name + "_" + db_mgr->getDatabaseFilePath();
            apps_[key] = std::unique_ptr<App>(app);
        }

        std::map<PipelineApp*, std::unique_ptr<PipelineConfig>> pipeline_configs;
        for (auto& [app_name, app] : apps_)
        {
            if (auto pipeline_app = dynamic_cast<PipelineApp*>(app.get()))
            {
                auto config = std::make_unique<PipelineConfig>();
                pipeline_app->configPipeline(*config);
                pipeline_app->setDatabaseManager(db_mgr);
                pipeline_configs[pipeline_app] = std::move(config);
            }
        }

        size_t total_async_stages = 0;
        for (const auto& [pipeline_app, config] : pipeline_configs)
        {
            total_async_stages = std::max(total_async_stages, config->numAsyncStages());
        }

        if (total_async_stages > 0)
        {
            auto pipeline = std::make_shared<Pipeline>();
            for (size_t i = 1; i <= total_async_stages; ++i)
            {
                auto stage = pipeline->addStage();
                if (i == total_async_stages)
                {
                    stage->setDatabaseManager(db_mgr);
                }
            }

            pipeline->finalize();
            for (const auto& [pipeline_app, config] : pipeline_configs)
            {
                size_t app_async_stages = config->numAsyncStages();
                if (app_async_stages > 0)
                {
                    //                |--------------------Non DB stage
                    //                |        |-----------Non DB stage
                    //                |        |        |--DB stage
                    //                |        |        |
                    //   Pipeline   Stage1   Stage2   Stage3
                    //   -----------------------------------------------------
                    //   app1 -----> entry -> entry -> entry (needs all 3 stages)
                    //   app2 --------------> entry -> entry (only needs 2 stages)
                    //
                    size_t first_stage_idx = 0;
                    size_t last_stage_idx = total_async_stages;

                    // app1
                    if (app_async_stages == total_async_stages)
                    {
                        first_stage_idx = 1;
                    }

                    // app2
                    else if (app_async_stages < total_async_stages)
                    {
                        first_stage_idx = total_async_stages - app_async_stages + 1;
                    }

                    // Get a vector of stage functions for this app.
                    //
                    // app1:
                    //   [
                    //      [Func1, Func2, Func3], // Stage 1
                    //      [Func4, Func5],        // Stage 2
                    //      [Func6]                // Stage 3
                    //   ]
                    //
                    // app2:
                    //   [
                    //      [],                    // Stage 1 (no functions)
                    //      [Func7, Func8],        // Stage 2
                    //      [Func9]                // Stage 3
                    //   ]
                    std::vector<std::vector<PipelineFunc>> stage_functions;
                    std::vector<PipelineStageObserver*> stage_observers;
                    size_t app_stage_idx = 1;
                    for (size_t stage_idx = 1; stage_idx <= total_async_stages; ++stage_idx)
                    {
                        if (stage_idx >= first_stage_idx && stage_idx <= last_stage_idx)
                        {
                            const auto& stage_config = config->asyncStage(app_stage_idx++);
                            stage_functions.push_back(stage_config.getFunctions());
                            stage_observers.push_back(stage_config.getObserver());
                        }
                        else
                        {
                            stage_functions.push_back({});
                        }
                    }

                    auto app_pipeline = std::make_unique<AppPipeline>(pipeline, stage_functions, stage_observers);
                    pipeline_app->setPipeline(std::move(app_pipeline));
                }
            }
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
                        continue; // App is associated with a different database
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

                    Schema app_schema;
                    if (app->defineSchema(app_schema))
                    {
                        db_mgr->appendSchema(app_schema);
                    }
                }
            });
    }

    /// Call this after command line args and config files are parsed.
    void postInit(DatabaseManager* db_mgr, int argc, char** argv)
    {
        db_mgr->safeTransaction(
            [&]()
            {
                for (const auto& [name, app] : apps_)
                {
                    app->postInit(argc, argv);
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
    void teardown()
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
    void destroy(DatabaseManager* db_mgr = nullptr)
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

        app_factories_.clear();
    }

private:
    /// Registered app factories.
    static inline std::map<std::string, std::unique_ptr<AppFactoryBase>> app_factories_;

    /// Instantiated apps (implicitly enabled).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    std::set<std::string> enabled_apps_;
};

} // namespace simdb
