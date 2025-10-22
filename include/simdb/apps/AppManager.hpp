// <AppManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/PipelineManager.hpp"

#include <map>
#include <set>
#include <iostream>

#define PROFILE_APP_PHASE ScopedTimer timer(__FUNCTION__, msg_log_); (void)timer;

namespace simdb {

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
        app_factories_[AppT::NAME] = std::make_shared<AppFactory<AppT>>();
    }

    /// Access an app factory.
    template <typename AppT>
    AppFactory<AppT>* getAppFactory(bool must_exist = true)
    {
        auto it = app_factories_.find(AppT::NAME);
        if (it == app_factories_.end())
        {
            if (must_exist)
            {
                throw DBException("No factory named ") << AppT::NAME << " exists.";
            }
            return nullptr;
        }

        auto factory = dynamic_cast<AppFactory<AppT>*>(it->second.get());
        if (!factory && must_exist)
        {
            throw DBException("Factory is not the expected subclass type");
        }

        return factory;
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
    void enableApp(const std::string& app_name, size_t num_instances = 1)
    {
        auto it = app_factories_.find(app_name);
        if (it == app_factories_.end())
        {
            throw DBException("No factory named ") << app_name << " exists.";
        }

        if (num_instances == 0)
        {
            throw DBException("App instances must be at least 1");
        }
        else if (num_instances > 1)
        {
            auto factory = it->second;
            for (size_t i = 1; i <= num_instances; ++i)
            {
                std::string instance_name = app_name + std::string("-") + std::to_string(i);
                app_factories_[instance_name] = factory;
            }
        }

        enabled_apps_[app_name] = num_instances;
    }

    template <typename AppT>
    void enableApp(size_t num_instances = 1)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        enableApp(AppT::NAME, num_instances);
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
        auto enabled_it = enabled_apps_.find(app_name);
        if (enabled_it == enabled_apps_.end())
        {
            return false;
        }

        auto num_instances = enabled_it->second;
        if (num_instances == 1)
        {
            return apps_.find(app_name) != apps_.end();
        }

        auto instantiated_count = 0;
        for (size_t i = 1; i <= num_instances; ++i)
        {
            std::string instance_name = app_name + std::string("-") + std::to_string(i);
            if (apps_.find(instance_name) != apps_.end())
            {
                ++instantiated_count;
            }
        }

        if (instantiated_count != num_instances)
        {
            throw DBException("App '") << app_name << "' expected " << num_instances
                                       << " instances, but only " << instantiated_count
                                       << " were found.";
        }

        return true;
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
        for (const auto& [app_name, num_instances] : enabled_apps_)
        {
            if (num_instances == 1)
            {
                App* app = app_factories_[app_name]->createApp(db_mgr_);
                apps_[app_name] = std::unique_ptr<App>(app);
            } else {
                for (size_t i = 1; i <= num_instances; ++i)
                {
                    App* app = app_factories_[app_name]->createApp(db_mgr_);
                    app->setInstance(i);
                    std::string instance_name = app_name + std::string("-") + std::to_string(i);
                    apps_[instance_name] = std::unique_ptr<App>(app);
                }
            }
        }
    }

    /// Get an instantiated app. Throws if not found.
    template <typename AppT>
    AppT* getApp(size_t instance = 0)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");

        auto it = apps_.find(AppT::NAME);
        if (it != apps_.end())
        {
            // If we found the base name, instance must be 0 (not a multi-instance app)
            if (instance != 0)
            {
                throw DBException("App ") << AppT::NAME << " is not a multi-instance app";
            }

            auto app = dynamic_cast<AppT*>(it->second.get());
            if (!app)
            {
                throw DBException("App of type ") << AppT::NAME << " is not of type " << typeid(AppT).name();
            }
            return app;
        } else {
            // Look for instance name
            std::string instance_name = AppT::NAME + std::string("-") + std::to_string(instance);
            it = apps_.find(instance_name);
            if (it != apps_.end())
            {
                auto app = dynamic_cast<AppT*>(it->second.get());
                if (!app)
                {
                    throw DBException("App of type ") << AppT::NAME << " is not of type " << typeid(AppT).name();
                }
                return app;
            }
        }

        // Throw if this is a multi-instance app and instance was not specified
        auto enabled_it = enabled_apps_.find(AppT::NAME);
        if (enabled_it != enabled_apps_.end())
        {
            auto num_instances = enabled_it->second;
            if (num_instances > 1 && instance == 0)
            {
                throw DBException("App ") << AppT::NAME << " is a multi-instance app; please specify instance number";
            }
        }

        return nullptr;
    }

    /// Create app-specific schemas for the instantiated apps.
    void createSchemas()
    {
        PROFILE_APP_PHASE

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

                    db_mgr_->INSERT(
                        SQL_TABLE("RegisteredApps"),
                        SQL_COLUMNS("AppName"),
                        SQL_VALUES(app_name));

                    Schema app_schema;
                    auto& factory = app_factories_[app_name];
                    factory->defineSchema(app_schema);
                    db_mgr_->appendSchema(app_schema);
                }
            });
    }

    /// Call this after command line args and config files are parsed.
    void postInit(int argc, char** argv)
    {
        PROFILE_APP_PHASE

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
        PROFILE_APP_PHASE

        if (pipeline_mgr_)
        {
            throw DBException("Pipelines already open");
        }

        pipeline_mgr_ = std::make_unique<pipeline::PipelineManager>(db_mgr_);
        for (const auto& [app_name, app] : apps_)
        {
            app->createPipeline(pipeline_mgr_.get());
        }

        pipeline_mgr_->openPipelines();

        // Print final thread/task configuration.
        msg_log_ << "\nSimDB app pipeline configuration for database '" << db_mgr_->getDatabaseFilePath() << "':\n";
        for (const auto pipeline : pipeline_mgr_->getPipelines())
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

    /// This method is to be called after the main simulation loop ends.
    /// It is assumed that there is no more data to be sent into your
    /// app's pipelines when you call this method. The following will
    /// be performed:
    ///
    ///   1. Call virtual App::preTeardown(). Needed for any apps which
    ///      use their own data structure to buffer data prior to sending
    ///      it down the pipeline, as they will have to push the remaining
    ///      data if they want it processed.
    ///
    ///   2. Stop all pipeline threads. Some data is still in the pipeline.
    ///
    ///   3. Call virtual TaskBase::processAll(force=true) for every task.
    ///      They will be called in the same order as they are organized
    ///      in their respective TaskGroup. Do this for all except the
    ///      DB writers.
    ///
    ///   4. Destroy all pipelines (and therefore task groups, tasks, and
    ///      their respective ConcurrentQueues).
    ///
    ///   5. Destroy all threads.
    ///
    ///   6. Call virtual App::postTeardown() in a BEGIN/COMMIT transaction.
    ///      This is the time to write your post-processing data, metadata,
    ///      and so on if you need to.
    ///
    void postSimLoopTeardown(bool delete_apps = false)
    {
        PROFILE_APP_PHASE

        for (auto app : getApps_())
        {
            app->preTeardown();
        }

        if (pipeline_mgr_)
        {
            pipeline_mgr_->postSimLoopTeardown(msg_log_);
        }

        db_mgr_->safeTransaction(
            [&]()
            {
                for (auto app : getApps_())
                {
                    app->postTeardown();
                }
            });

        if (delete_apps)
        {
            destroyAllApps();
        }
    }

    /// Delete all instantiated apps. This may be needed since AppManager
    /// is a singleton and your simulator might want to call app destructors
    /// before the AppManager itself is destroyed on program exit.
    void destroyAllApps()
    {
        pipeline_mgr_.reset();
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
    static inline std::map<std::string, std::shared_ptr<AppFactoryBase>> app_factories_;

    /// Instantiated apps (implicitly enabled).
    /// Key is the App's NAME static member, or NAME-<instance>
    /// for apps with multiple instances (one-based).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    /// Key is the App's NAME static member.
    /// Value is the number of instances to create.
    std::map<std::string, size_t> enabled_apps_;

    /// Associated database.
    DatabaseManager* db_mgr_ = nullptr;

    /// All pipelines and threads are managed by PipelineManager.
    std::unique_ptr<pipeline::PipelineManager> pipeline_mgr_;

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
