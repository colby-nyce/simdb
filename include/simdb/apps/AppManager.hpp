#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/PipelineThread.hpp"

#include <map>
#include <set>

namespace simdb
{
class PipelineThread;

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
                configureAppPipelines_(db_mgr);
                for (auto app : getApps_(db_mgr))
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
                for (auto app : getApps_(db_mgr))
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
        auto db_apps = getApps_(db_mgr);
        if (db_apps.empty())
        {
            return;
        }

        for (auto& thread : pipeline_threads_[db_mgr])
        {
            thread->flush();
        }

        for (auto& thread : pipeline_threads_[db_mgr])
        {
            thread->stopThreadLoop();
        }

        for (auto app : db_apps)
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
    /// Get all Apps that belong to the given database.
    std::vector<App*> getApps_(DatabaseManager* db_mgr)
    {
        std::vector<App*> apps;
        for (const auto& [key, app] : apps_)
        {
            // The key is formatted as "<AppName>_<db_file>"
            auto pos = key.find("_" + db_mgr->getDatabaseFilePath());
            if (pos != std::string::npos)
            {
                apps.push_back(app.get());
            }
        }
        return apps;
    }

    /// Get all PipelineApps that belong to the given database.
    std::vector<PipelineApp*> getPipelineApps_(DatabaseManager* db_mgr)
    {
        std::vector<PipelineApp*> pipeline_apps;
        for (auto app : getApps_(db_mgr))
        {
            if (auto pipeline_app = dynamic_cast<PipelineApp*>(app))
            {
                pipeline_apps.push_back(pipeline_app);
            }            
        }
        return pipeline_apps;
    }

    /// Configure all pipelines used for this database's running applications.
    /// This is called as late as possible in postInit() right before simulation
    /// starts.
    void configureAppPipelines_(DatabaseManager* db_mgr)
    {
        std::vector<PipelineApp*> db_apps = getPipelineApps_(db_mgr);
        if (db_apps.empty())
        {
            return;
        }

        auto& pipeline_threads = pipeline_threads_[db_mgr];
        if (!pipeline_threads.empty())
        {
            throw DBException("Reconfiguring pipeline is disallowed");
        }

        std::map<PipelineApp*, std::unique_ptr<PipelineConfig>> configs;
        for (auto app : db_apps) {
            auto cfg = std::make_unique<PipelineConfig>();
            for (auto& stage : app->configPipeline())
            {
                cfg->addStage(std::move(stage));
            }
            configs[app] = std::move(cfg);
        }

        std::vector<PipelineConfig::Config> app_configs;
        for (auto& [app, cfg] : configs)
        {
            app_configs.emplace_back(cfg->getConfig());
        }

        // We need to give each app its specific ConcurrentQueue
        // where it should send its data down the pipeline.
        for (auto& [app, cfg] : configs) {
            app->setPipelineInputQueue(cfg->getInputQueue());
        }

        // Create one PipelineThread object per stage, and give all the 
        // apps' stages to the appropriate PipelineThread.
        bool needs_db_stage = false;
        size_t num_non_db_stages = 0;
        for (const auto& cfg : app_configs) {
            if (cfg.needs_db)
            {
                needs_db_stage = true;
                if (cfg.num_stages > 1)
                {
                    num_non_db_stages = std::max(num_non_db_stages, cfg.num_stages - 1);
                }
            }
            else
            {
                num_non_db_stages = std::max(num_non_db_stages, cfg.num_stages);
            }
        }

        const size_t num_stages = (needs_db_stage ? 1 : 0) + num_non_db_stages;
        for (size_t i = 0; i < num_stages; ++i)
        {
            pipeline_threads.emplace_back(std::make_unique<PipelineThread>());
        }

        std::map<PipelineApp*, std::vector<std::unique_ptr<PipelineStageBase>>> app_stages;
        for (auto& [app, cfg] : configs) {
            app_stages[app] = cfg->releaseStages();
        }

        std::map<PipelineThread*, std::vector<std::unique_ptr<PipelineStageBase>>> thread_stages;

        // Add final stages to the DB thread
        size_t thread_idx = num_stages - 1;
        if (needs_db_stage)
        {
            for (auto& [app, stages] : app_stages)
            {
                if (auto db = stages.back()->getDatabaseManager())
                {
                    assert(db_mgr == db); (void)db;
                    thread_stages[pipeline_threads[thread_idx].get()].emplace_back(std::move(stages.back()));
                    pipeline_threads[thread_idx]->setDatabaseManager(db_mgr);
                    stages.pop_back();
                }
            }
            --thread_idx;
        }

        // Add remaining stages to non-DB threads
        for (size_t i = 0; i < num_non_db_stages; ++i)
        {
            for (auto& [app, stages] : app_stages)
            {
                if (!stages.empty())
                {
                    thread_stages[pipeline_threads[thread_idx].get()].emplace_back(std::move(stages.back()));
                    stages.pop_back();
                }
            }

            if (thread_idx == 0)
            {
                assert(i == num_non_db_stages - 1);
                break;
            }
        }

        // Sanity check
        for (auto& [app, stages] : app_stages)
        {
            if (!stages.empty())
            {
                throw DBException("Internal logic error");
            }
        }

        // Finalize threads.
        for (auto& [thread, stages] : thread_stages)
        {
            // Reverse is needed since we "walked backwards" setting up
            // the thread stages (DB stage first, then walk backwards).
            std::reverse(stages.begin(), stages.end());
            for (auto& stage : stages)
            {
                thread->addStage(std::move(stage));
            }
        }

        // Connect all input/output queues.
        std::vector<PipelineTransformBase*> all_transforms;
        for (auto& thread : pipeline_threads)
        {
            auto thread_transforms = thread->getTransforms();
            all_transforms.insert(all_transforms.end(), thread_transforms.begin(), thread_transforms.end());
        }

        for (size_t i = 1; i < all_transforms.size(); ++i)
        {
            auto prev = all_transforms[i-1];
            auto curr = all_transforms[i];
            prev->setOutputQueue(curr->getInputQueue());
        }

        // Open threads
        for (auto& thread : pipeline_threads)
        {
            thread->startThreadLoop();
        }
    }

    /// Registered app factories.
    static inline std::map<std::string, std::unique_ptr<AppFactoryBase>> app_factories_;

    /// Instantiated apps (implicitly enabled).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    std::set<std::string> enabled_apps_;

    /// All threads for all pipelines for all apps.
    std::map<DatabaseManager*, std::vector<std::unique_ptr<PipelineThread>>> pipeline_threads_;
};

} // namespace simdb
