#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PipelineThread.hpp"

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
                for (auto app : getApps_(db_mgr))
                {
                    app->postInit(argc, argv);
                }
            });
    }

    /// Call this once after all postInit().
    void openPipelines()
    {
        for (const auto& [key, app] : apps_)
        {
            if (auto pipeline = app->createPipeline())
            {
                // For pipelines which require the DB, verify that their DB tasks all occur
                // at the end of the pipeline. This may be relaxed in the future.
                if (pipeline->requiresDatabase())
                {
                    bool processing_db_tasks = true;
                    auto all_tasks = pipeline->getTasks<>();
                    for (auto it = all_tasks.rbegin(); it != all_tasks.rend(); ++it)
                    {
                        if (dynamic_cast<pipeline::DatabaseTask*>(*it))
                        {
                            if (!processing_db_tasks)
                            {
                                throw DBException("Database tasks must be at the end of the pipeline");
                            }
                        }
                        else
                        {
                            processing_db_tasks = false;
                        }
                    }
                }

                pipelines_[pipeline->getDatabaseManager()].emplace_back(std::move(pipeline));
            }
        }

        std::vector<pipeline::Thread*> all_threads;

        // Setup all the database threads we will need.
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            for (auto& pipeline : pipelines)
            {
                if (pipeline->requiresDatabase())
                {
                    if (pipeline->getDatabaseManager() != db_mgr)
                    {
                        throw DBException("Database mismatch");
                    }

                    auto& db_thread = db_threads_[db_mgr];
                    if (!db_thread)
                    {
                        db_thread = std::make_unique<pipeline::DatabaseThread>(db_mgr);
                        all_threads.emplace_back(db_thread.get());
                    }

                    for (auto task : pipeline->getTasks<pipeline::DatabaseTask>())
                    {
                        task->setDatabaseManager(db_mgr);
                        db_thread->addRunnable(task);
                    }
                }
            }
        }

        // Setup all the non-database threads
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            size_t num_non_db_threads_needed = 0;
            for (auto& pipeline : pipelines)
            {
                auto all_tasks = pipeline->getTasks<>().size();
                auto db_tasks = pipeline->getTasks<pipeline::DatabaseTask>().size();
                assert(all_tasks >= db_tasks);
                num_non_db_threads_needed = std::max(num_non_db_threads_needed, all_tasks - db_tasks);
            }

            for (size_t i = 0; i < num_non_db_threads_needed; ++i)
            {
                non_db_threads_[db_mgr].emplace_back(std::make_unique<pipeline::Thread>());
                all_threads.emplace_back(non_db_threads_[db_mgr].back().get());
            }
        }

        // Add the non-database tasks to the appropriate thread.
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            for (auto& pipeline : pipelines)
            {
                size_t thread_idx = 0;
                for (auto task : pipeline->getTasks<>())
                {
                    if (!dynamic_cast<pipeline::DatabaseTask*>(task))
                    {
                        non_db_threads_[db_mgr][thread_idx++]->addRunnable(task);
                    }
                }
            }
        }

        for (auto& thread : all_threads)
        {
            thread->open();
        }
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
    /// such as closing files, releasing memory, etc.
    void teardown(DatabaseManager* db_mgr)
    {
        for (auto app : getApps_(db_mgr))
        {
            app->teardown();
        }

        auto it = non_db_threads_.find(db_mgr);
        if (it != non_db_threads_.end())
        {
            non_db_threads_.erase(it);
        }

        auto it2 = db_threads_.find(db_mgr);
        if (it2 != db_threads_.end())
        {
            db_threads_.erase(it2);
        }

        auto it3 = pipelines_.find(db_mgr);
        if (it3 != pipelines_.end())
        {
            pipelines_.erase(it3);
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
            non_db_threads_.clear();
            db_threads_.clear();
            pipelines_.clear();
        }
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

    /// Registered app factories.
    static inline std::map<std::string, std::unique_ptr<AppFactoryBase>> app_factories_;

    /// Instantiated apps (implicitly enabled).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    std::set<std::string> enabled_apps_;

    /// Instantiated pipelines.
    std::map<DatabaseManager*, std::vector<std::unique_ptr<pipeline::Pipeline>>> pipelines_;

    /// Instantiated threads.
    std::map<DatabaseManager*, std::unique_ptr<pipeline::DatabaseThread>> db_threads_;
    std::map<DatabaseManager*, std::vector<std::unique_ptr<pipeline::Thread>>> non_db_threads_;
};

} // namespace simdb
