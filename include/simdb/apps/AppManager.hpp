// <AppManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/Pipeline.hpp"
#include "simdb/pipeline/PollingThread.hpp"
#include "simdb/pipeline/DatabaseThread.hpp"

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
        ScopedTimer timer("createSchemas()", msg_log_); (void)timer;

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
        ScopedTimer timer("postInit()", msg_log_); (void)timer;

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

        ScopedTimer timer("openPipelines()", msg_log_); (void)timer;

        polling_threads_.emplace_back(std::make_unique<pipeline::DatabaseThread>(db_mgr_));
        auto db_thread = dynamic_cast<pipeline::DatabaseThread*>(polling_threads_.back().get());
        auto db_accessor = db_thread->getAsyncDatabaseAccessor();
        for (const auto& [app_name, app] : apps_)
        {
            if (auto pipeline = app->createPipeline(db_accessor))
            {
                pipelines_.emplace_back(std::move(pipeline));
            }
        }

        // The number of non-database processing threads we need is equal to
        // the max number of TaskGroups across all our pipelines.
        size_t num_proc_threads = 0;
        for (auto& pipeline : pipelines_)
        {
            num_proc_threads = std::max(num_proc_threads, pipeline->getTaskGroups().size());
        }

        for (size_t i = 0; i < num_proc_threads; ++i)
        {
            polling_threads_.emplace_back(std::make_unique<pipeline::PollingThread>());
        }

        for (auto& pipeline : pipelines_)
        {
            auto thread_it = polling_threads_.begin() + 1; // Advance over database thread
            for (auto group : pipeline->getTaskGroups())
            {
                if (thread_it == polling_threads_.end())
                {
                    throw DBException("Internal logic error while connecting threads and runnables");
                }

                (*thread_it)->addRunnable(group);
                ++thread_it;
            }
        }

        for (auto& thread : polling_threads_)
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
        ScopedTimer timer("postSim()", msg_log_); (void)timer;

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
        ScopedTimer timer("teardown()", msg_log_); (void)timer;

        // Wait until the pipeline is finished
        std::vector<const simdb::pipeline::QueueBase*> input_queues;
        for (const auto& pipeline : pipelines_)
        {
            for (const auto group : pipeline->getTaskGroups())
            {
                for (const auto task : group->getTasks())
                {
                    if (auto q = task->getInputQueue())
                    {
                        input_queues.push_back(q);
                    }
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

        for (auto& thread : polling_threads_)
        {
            thread->close();
            std::ostringstream oss;
            thread->printPerfReport(oss);
            msg_log_ << oss.str() << "\n\n";
        }
        polling_threads_.clear();

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
        ScopedTimer timer("destroy()", msg_log_); (void)timer;

        assert(polling_threads_.empty());
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
    std::vector<std::unique_ptr<pipeline::PollingThread>> polling_threads_;

    /// Associated database.
    DatabaseManager* db_mgr_ = nullptr;

    /// RAII timer to measure the performance of teardown()
    class ScopedTimer
    {
    public:
        ScopedTimer(const std::string& block_name, std::ostream* msg_out = &std::cout)
            : start_(std::chrono::high_resolution_clock::now())
            , block_name_(block_name)
            , msg_out_(msg_out)
        {
            *msg_out_ << "SimDB: Entering " << block_name << "\n";
        }

        ~ScopedTimer()
        {
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> dur = end - start_;
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
            if (us >= 1000000)
            {
                auto sec = (double)us / 1000000;
                *msg_out_ << "SimDB: Completed " << block_name_ << " in ";
                *msg_out_ << std::fixed << std::setprecision(2) << sec << " seconds.\n";
            }
            else if (us >= 1000)
            {
                auto milli = (double)us / 1000;
                *msg_out_ << "SimDB: Completed " << block_name_ << " in ";
                *msg_out_ << std::fixed << std::setprecision(0) << milli << " milliseconds.\n";
            }
            else
            {
                auto micro = (double)us;
                *msg_out_ << "SimDB: Completed " << block_name_ << " in ";
                *msg_out_ << std::fixed << std::setprecision(0) << micro << " microseconds.\n";
            }
        }
    
    private:
        std::chrono::high_resolution_clock::time_point start_;
        std::string block_name_;
        std::ostream* msg_out_ = nullptr;
    };

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

        operator std::ostream*()
        {
            return out_;
        }

    private:
        std::ostream* out_ = nullptr;
    };

    Logger msg_log_;
    Logger err_log_;
};

} // namespace simdb
