// <AppManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/Thread.hpp"

#include <map>
#include <set>
#include <iostream>

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

    /// AppManagers are associated 1-to-1 with a DatabaseManager.
    AppManager(DatabaseManager* db_mgr, std::ostream* msg_log = &std::cout, std::ostream* err_log = &std::cerr)
        : db_mgr_(db_mgr)
        , msg_log_(msg_log)
        , err_log_(err_log)
    {}

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

    template <typename AppT>
    void enableApp()
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        enableApp(AppT::NAME);
    }

    /// Check if your app is enabled (might not be instantiated yet).
    bool enabled(const std::string& app_name) const
    {
        return enabled_apps_.find(app_name) != enabled_apps_.end();
    }

    /// Check if your app is enabled (might not be instantiated yet).
    template <typename AppT>
    bool enabled() const
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        return enabled(AppT::NAME);
    }

    /// Check if your app is instantiated (enabled and created).
    bool instantiated(const std::string& app_name) const
    {
        return apps_.find(app_name) != apps_.end();
    }

    /// Check if your app is instantiated (enabled and created).
    template <typename AppT>
    bool instantiated() const
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        return instantiated(AppT::NAME);
    }

    /// Call after command line args and config files are parsed.
    void createEnabledApps()
    {
        for (const auto& app_name : enabled_apps_)
        {
            App* app = app_factories_[app_name]->createApp(db_mgr_);
            apps_[app_name] = std::unique_ptr<App>(app);
        }
    }

    /// Get an instantiated app. Throws if not found.
    template <typename AppT>
    AppT* getApp(bool must_exist = true)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");

        auto it = apps_.find(AppT::NAME);
        if (it != apps_.end())
        {
            auto app = dynamic_cast<AppT*>(it->second.get());
            if (!app && must_exist)
            {
                throw DBException("App of type ") << AppT::NAME << " is not of type " << typeid(AppT).name();
            }
            return app;
        }
        else if (must_exist)
        {
            throw DBException("App not found: ") << AppT::NAME;
        }

        return nullptr;
    }

    /// Create app-specific schemas for the instantiated apps.
    void createSchemas()
    {
        db_mgr_->safeTransaction(
            [&]()
            {
                Schema schema;
                using dt = SqlDataType;

                auto& tbl = schema.addTable("RegisteredApps");
                tbl.addColumn("AppName", dt::string_t);
                db_mgr_->appendSchema(schema);

                for (const auto& [app_name, app] : apps_)
                {
                    // Ensure no duplicates
                    auto query = db_mgr_->createQuery("RegisteredApps");
                    query->addConstraintForString("AppName", Constraints::EQUAL, app_name);
                    if (query->count() > 0)
                    {
                        throw DBException("App already registered: ") << app_name;
                    }

                    auto record = db_mgr_->INSERT(
                        SQL_TABLE("RegisteredApps"),
                        SQL_COLUMNS("AppName"),
                        SQL_VALUES(app_name));

                    app->app_id_ = record->getId();

                    Schema app_schema;
                    if (app->defineSchema(app_schema))
                    {
                        db_mgr_->appendSchema(app_schema);
                    }
                }
            });
    }

    /// Call this after command line args and config files are parsed.
    void postInit(int argc, char** argv)
    {
        db_mgr_->safeTransaction(
            [&]()
            {
                for (auto app : getApps_())
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

        for (const auto& [app_name, app] : apps_)
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

                pipelines_.emplace_back(std::move(pipeline));
            }
        }

        // Figure out how many threads we need for pre-database task groups. If one
        // app needs 3 pre-DB TaskGroups, and another needs 2, then we will create 3
        // threads total for pre-DB task groups and share them.
        size_t num_pre_db_threads_needed = 0;
        for (auto& pipeline : pipelines_)
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

            num_pre_db_threads_needed = std::max(num_pre_db_threads_needed, num_pre_db_threads);
        }

        // Figure out how many threads we need for DB task groups.
        size_t num_db_threads_needed = 0;
        for (auto& pipeline : pipelines_)
        {
            if (pipeline->requiresDatabase())
            {
                num_db_threads_needed = 1;
            }
        }

        // Figure out how many threads we need for post-DB task groups.
        size_t num_post_db_threads_needed = 0;
        for (auto& pipeline : pipelines_)
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

            num_post_db_threads_needed = std::max(num_post_db_threads_needed, num_post_db_threads);
        }

        for (size_t i = 0; i < num_pre_db_threads_needed; ++i)
        {
            threads_.emplace_back(std::make_unique<pipeline::Thread>());
        }

        if (num_db_threads_needed)
        {
            assert(num_db_threads_needed == 1);
            threads_.emplace_back(std::make_unique<pipeline::DatabaseThread>(db_mgr_));
        }

        for (size_t i = 0; i < num_post_db_threads_needed; ++i)
        {
            threads_.emplace_back(std::make_unique<pipeline::Thread>());
        }

        for (auto& pipeline : pipelines_)
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

            auto thread_it = threads_.begin();
            for (auto group : pre_db_groups)
            {
                if (thread_it == threads_.end())
                {
                    throw DBException("Internal logic error while connecting threads and runnables");
                }

                (*thread_it)->addRunnable(group);
                ++thread_it;
            }

            if (db_group)
            {
                while (thread_it != threads_.end())
                {
                    if (dynamic_cast<pipeline::DatabaseThread*>(thread_it->get()))
                    {
                        break;
                    }
                    ++thread_it;
                }

                if (thread_it == threads_.end())
                {
                    throw DBException("Internal logic error while connecting threads and runnables");
                }

                db_group->setDatabaseManager(db_mgr_);
                (*thread_it)->addRunnable(db_group);
                ++thread_it;
            }

            for (auto group : post_db_groups)
            {
                if (thread_it == threads_.end())
                {
                    throw DBException("Internal logic error while connecting threads and runnables");
                }

                (*thread_it)->addRunnable(group);
                ++thread_it;
            }
        }

        for (auto& thread : threads_)
        {
            thread->open();
        }

        // Print final thread/task configuration.
        msg_log_ << "\nSimDB app pipeline configuration for database '" << db_mgr_->getDatabaseFilePath() << "':\n";
        for (auto& pipeline : pipelines_)
        {
            msg_log_ << "---- Pipeline (app): " << pipeline->getName() << "\n";
            for (auto group : pipeline->getTaskGroups())
            {
                msg_log_ << "------ TaskGroup (thread): " << group->getDescription() << "\n";
                for (auto task : group->getTasks())
                {
                    msg_log_ << "-------- Task: " << task->getDescription() << "\n";
                }
            }
        }

        msg_log_ << "\n";
    }

    /// Call this after the simulation loop ends for post-processing tasks.
    void postSim()
    {
        msg_log_ << "************ Shutting down pipelines for all SimDB apps on database: "
                 << db_mgr_->getDatabaseFilePath() << " ************\n\n";

        // Wait until the pipeline is finished
        std::vector<const simdb::pipeline::QueueBase*> input_queues;
        for (const auto& pipeline : pipelines_)
        {
            for (const auto group : pipeline->getTaskGroups())
            {
                for (const auto task : group->getTasks())
                {
                    input_queues.push_back(task->getInputQueue());
                }
            }
        }

        while (true)
        {
            bool all_empty = true;
            for (const auto q : input_queues)
            {
                if (q->size() > 0)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    all_empty = false;
                    break;
                }
            }

            if (all_empty)
            {
                break;
            }
        }

        for (auto& thread : threads_)
        {
            thread->close();
            std::ostringstream oss;
            thread->printPerfReport(oss);
            msg_log_ << oss.str() << "\n\n";
        }
        threads_.clear();

        db_mgr_->safeTransaction(
            [&]()
            {
                for (auto app : getApps_())
                {
                    app->postSim();
                }
            });
    }

    /// Call this after the simulation ends for resource cleanup tasks
    /// such as closing files, releasing memory, etc.
    void teardown()
    {
        assert(threads_.empty());
        for (auto app : getApps_())
        {
            app->teardown();
        }
        pipelines_.clear();
    }

    /// Delete all instantiated apps. This may be needed since AppManager
    /// is a singleton and your simulator might want to call app destructors
    /// before the AppManager itself is destroyed on program exit.
    void destroy()
    {
        assert(threads_.empty());
        assert(pipelines_.empty());
        apps_.clear();
    }

private:
    /// Get all Apps that belong to the given database.
    std::vector<App*> getApps_()
    {
        std::vector<App*> apps;
        for (const auto& [key, app] : apps_)
        {
            apps.push_back(app.get());
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
    std::vector<std::unique_ptr<pipeline::Pipeline>> pipelines_;

    /// Instantiated threads.
    std::vector<std::unique_ptr<pipeline::Thread>> threads_;

    /// Associated database.
    DatabaseManager* db_mgr_ = nullptr;

    /// Simple wrapper around std::ostream* for conditional logging
    class Logger
    {
    public:
        Logger(std::ostream* out) : out_(out) {}

        template <typename T>
        Logger& operator<<(const T& msg)
        {
            if (out_)
            {
                *out_ << msg;
                out_->flush();
            }
            return *this;
        }

        Logger& operator<<(const char* msg)
        {
            if (out_)
            {
                *out_ << msg;
                out_->flush();
            }
            return *this;
        }

    private:
        std::ostream* out_ = nullptr;
    };

    Logger msg_log_;
    Logger err_log_;
};

} // namespace simdb
