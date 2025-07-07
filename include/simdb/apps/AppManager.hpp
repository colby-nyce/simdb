// <AppManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/Thread.hpp"

#include <map>
#include <set>

namespace simdb
{

/// This class is responsible for registering, enabling, instantiating,
/// and managing the lifecycle of all SimDB applications running in a
/// simulation.
class AppManager
{
public:
    /// Register an app as early as possible (doesn't create it yet).
    /// Register your app by adding this macro to your source file:
    ///
    ///   class MyApp : public simdb::App { ... };     <<< header
    ///   REGISTER_SIMDB_APPLICATION(MyApp);           <<< source
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

    /// Call this once after all postInit(). This will create all pipelines
    /// for all enabled apps, and share resources between them to not create
    /// too many threads in total.
    void openPipelines()
    {
        if (!pipelines_.empty())
        {
            throw DBException("Pipelines already open");
        }

        for (const auto& [key, app] : apps_)
        {
            if (auto pipeline = app->createPipeline())
            {
                // Verify that there is at most one TaskGroup that is configured to
                // work with the database. We don't allow pipelines to try to access
                // the database (DatabaseManager) from multiple threads.
                size_t num_db_threads = 0;
                for (auto group : pipeline->getTaskGroups())
                {
                    num_db_threads += group->requiresDatabase() ? 1 : 0;
                }

                if (num_db_threads > 1)
                {
                    throw DBException() <<
                        "You cannot access the DatabaseManager from multiple TaskGroups " <<
                        "(at most one DatabaseThread per pipeline)";
                }

                pipelines_[pipeline->getDatabaseManager()].emplace_back(std::move(pipeline));
            }
        }

        // Figure out how many threads we need for pre-database task groups. If one
        // app needs 3 pre-DB TaskGroups, and another needs 2, then we will create 3
        // threads total for pre-DB task groups and share them.
        std::map<DatabaseManager*, size_t> num_pre_db_threads_needed;
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            for (auto& pipeline : pipelines)
            {
                size_t num_pre_db_threads = 0;
                for (auto group : pipeline->getTaskGroups())
                {
                    if (!group->requiresDatabase())
                    {
                        ++num_pre_db_threads;
                    }
                    else
                    {
                        break;
                    }
                }

                auto it = num_pre_db_threads_needed.find(db_mgr);
                if (it == num_pre_db_threads_needed.end())
                {
                    it = num_pre_db_threads_needed.insert({db_mgr, num_pre_db_threads}).first;
                }

                it->second = std::max(it->second, num_pre_db_threads);
            }
        }

        // Figure out how many threads we need for DB task groups.
        std::map<DatabaseManager*, size_t> num_db_threads_needed;
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            num_db_threads_needed[db_mgr] = 0;
            for (auto& pipeline : pipelines)
            {
                if (pipeline->requiresDatabase())
                {
                    num_db_threads_needed[db_mgr] = 1;
                    break;
                }
            }
        }

        // Figure out how many threads we need for post-DB task groups.
        std::map<DatabaseManager*, size_t> num_post_db_threads_needed;
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            for (auto& pipeline : pipelines)
            {
                size_t num_post_db_threads = 0;
                bool found_db_group = false;
                for (auto group : pipeline->getTaskGroups())
                {
                    if (found_db_group && !group->requiresDatabase())
                    {
                        ++num_post_db_threads;
                    }
                    else if (found_db_group && group->requiresDatabase())
                    {
                        throw DBException() <<
                            "You cannot access the DatabaseManager from multiple TaskGroups " <<
                            "(at most one DatabaseThread per pipeline)";
                    }
                    else if (group->requiresDatabase())
                    {
                        found_db_group = true;
                    }
                }

                auto it = num_post_db_threads_needed.find(db_mgr);
                if (it == num_post_db_threads_needed.end())
                {
                    it = num_post_db_threads_needed.insert({db_mgr, num_post_db_threads}).first;
                }

                it->second = std::max(it->second, num_post_db_threads);
            }
        }

        auto get_count = [](const auto& map, DatabaseManager* db_mgr) -> size_t
        {
            auto it = map.find(db_mgr);
            if (it == map.end())
            {
                throw DBException("Invalid map/key");
            }
            return it->second;
        };

        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            auto num_pre_db_threads = get_count(num_pre_db_threads_needed, db_mgr);
            auto num_db_threads = get_count(num_db_threads_needed, db_mgr);
            auto num_post_db_threads = get_count(num_post_db_threads_needed, db_mgr);

            for (size_t i = 0; i < num_pre_db_threads; ++i)
            {
                threads_[db_mgr].emplace_back(std::make_unique<pipeline::Thread>());
            }

            if (num_db_threads)
            {
                assert(num_db_threads == 1);
                threads_[db_mgr].emplace_back(std::make_unique<pipeline::DatabaseThread>(db_mgr));
            }

            for (size_t i = 0; i < num_post_db_threads; ++i)
            {
                threads_[db_mgr].emplace_back(std::make_unique<pipeline::Thread>());
            }
        }

        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            for (auto& pipeline : pipelines)
            {
                std::vector<pipeline::TaskGroup*> groups = pipeline->getTaskGroups();

                std::vector<pipeline::TaskGroup*> pre_db_groups;
                for (auto group : groups)
                {
                    if (!group->requiresDatabase())
                    {
                        pre_db_groups.push_back(group);
                    }
                    else
                    {
                        break;
                    }
                }

                pipeline::TaskGroup* db_group = nullptr;
                for (auto group : groups)
                {
                    if (group->requiresDatabase())
                    {
                        assert(db_group == nullptr);
                        db_group = group;
                    }
                }

                std::vector<pipeline::TaskGroup*> post_db_groups;
                bool found_db_group = false;
                for (auto group : groups)
                {
                    if (group->requiresDatabase())
                    {
                        found_db_group = true;
                    }
                    else if (found_db_group)
                    {
                        post_db_groups.push_back(group);
                    }
                }

                auto thread_it = threads_[db_mgr].begin();
                for (auto group : pre_db_groups)
                {
                    if (thread_it == threads_[db_mgr].end())
                    {
                        throw DBException("Internal logic error while connecting threads and runnables");
                    }

                    (*thread_it)->addRunnable(group);
                    ++thread_it;
                }

                if (db_group)
                {
                    while (thread_it != threads_[db_mgr].end())
                    {
                        if (dynamic_cast<pipeline::DatabaseThread*>(thread_it->get()))
                        {
                            break;
                        }
                        ++thread_it;
                    }
                    
                    if (thread_it == threads_[db_mgr].end())
                    {
                        throw DBException("Internal logic error while connecting threads and runnables");
                    }

                    db_group->setDatabaseManager(db_mgr);
                    (*thread_it)->addRunnable(db_group);
                    ++thread_it;
                }

                for (auto group : post_db_groups)
                {
                    if (thread_it == threads_[db_mgr].end())
                    {
                        throw DBException("Internal logic error while connecting threads and runnables");
                    }

                    (*thread_it)->addRunnable(group);
                    ++thread_it;
                }
            }
        }

        for (auto& [db_mgr, threads] : threads_)
        {
            for (auto& thread : threads)
            {
                thread->open();
            }
        }

        // Print final thread/task configuration.
        std::cout << "\nSimDB app pipeline configuration" << (pipelines_.size() == 1 ? ":" : "s:") << "\n";
        for (auto& [db_mgr, pipelines] : pipelines_)
        {
            std::cout << "-- Database: " << db_mgr->getDatabaseFilePath() << "\n";
            for (auto& pipeline : pipelines)
            {
                std::cout << "---- Pipeline (app): " << pipeline->getName() << "\n";
                for (auto group : pipeline->getTaskGroups())
                {
                    std::cout << "------ TaskGroup (thread): " << group->getDescription() << "\n";
                    for (auto task : group->getTasks())
                    {
                        std::cout << "-------- Task: " << task->getDescription() << "\n";
                    }
                }
            }
        }
    }

    /// Call this after the simulation loop ends for post-processing tasks.
    void postSim(DatabaseManager* db_mgr)
    {
        std::cout << "************ Shutting down pipelines for all SimDB apps on database: "
                  << db_mgr->getDatabaseFilePath() << " ************\n\n";

        auto it = threads_.find(db_mgr);
        if (it != threads_.end())
        {
            for (auto& thread : it->second)
            {
                thread->close();
            }
        }

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

        auto it = threads_.find(db_mgr);
        if (it != threads_.end())
        {
            for (const auto& pipeline : it->second)
            {
                pipeline->printPerfReport(std::cout);
            }
            threads_.erase(it);
        }

        auto it2 = pipelines_.find(db_mgr);
        if (it2 != pipelines_.end())
        {
            pipelines_.erase(it2);
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
            threads_.clear();
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
    std::map<DatabaseManager*, std::vector<std::unique_ptr<pipeline::Thread>>> threads_;
};

} // namespace simdb
