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

namespace utils {
    template <typename, typename = void>
    struct has_nested_factory : std::false_type{};

    template <typename T>
    struct has_nested_factory<T, std::void_t<typename T::AppFactory>> : std::true_type{};
}

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
        auto & app_factories = getAppFactories_();
        auto & factory = app_factories[AppT::NAME];
        if (factory)
        {
            throw DBException("App already registered: ") << AppT::NAME;
        }

        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            factory = std::make_shared<typename AppT::AppFactory>();
        }
        else
        {
            factory = std::make_shared<AppFactory<AppT>>();
        }
    }

    /// Parameterize an app factory. Call this before createEnabledApps().
    /// Your app subclass must have a public nested class called "AppFactory",
    /// inheriting publicly from simdb::AppFactoryBase. Your nested AppFactory
    /// needs to have a method with this signature:
    ///
    ///   void parameterize(size_t instance_num, /*rest, of, args, ...*/)
    ///   {
    ///       ...
    ///   }
    ///
    /// For example:
    ///
    ///   class MyApp : public simdb::App
    ///   {
    ///   public:
    ///       MyApp(simdb::DatabaseManager* db_mgr, int x, float y);
    ///
    ///       class AppFactory : public simdb::AppFactoryBase
    ///       {
    ///       public:
    ///           void parameterize(size_t instance_num, int x, float y)
    ///           {
    ///               ctor_args_[instance_num] = std::make_pair(x, y);
    ///           }
    ///
    ///           simdb::App* createApp(simdb::DatabaseManager* db_mgr, size_t instance_num) override
    ///           {
    ///               const auto & [x, y] = ctor_args_.at(instance_num);
    ///               return new MyApp(db_mgr, x, y);
    ///           }
    ///
    ///           void defineSchema(simdb::Schema& schema) const override
    ///           {
    ///               ...
    ///           }
    ///
    ///       private:
    ///           std::map<size_t, std::pair<int,float>> ctor_args_;
    ///       };
    ///   };
    ///
    /// Then do this:
    ///
    ///   // Assume have the AppManager
    ///   app_mgr.enableApp(MyApp::NAME);
    ///   app_mgr.enableApp(MyApp::NAME, 2 /*inst count*/);
    ///
    ///   // Parameterize before createEnabledApps()
    ///   AppManager::parameterizeAppFactory<MyApp>(1, 2.2);
    ///   AppManager::parameterizeAppFactoryInstance<MyApp>(1 /*inst num*/, 1, 2.2);
    ///   AppManager::parameterizeAppFactoryInstance<MyApp>(2 /*inst num*/, 3, 4.4);
    ///
    ///   // Then continue and create the apps:
    ///   app_mgr.createEnabledApps();
    template <typename AppT, typename... Args>
    static void parameterizeAppFactory([[maybe_unused]] Args&&... args)
    {
        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            auto factory = getAppFactory_<AppT>();
            factory->parameterize(0, std::forward<Args>(args)...);
        }
        else
        {
            throw DBException("No nested class 'AppFactory' exists for app '")
                << AppT::NAME << "'.";
        }
    }

    /// Parameterize an app factory. Call this after enableApp() but before
    /// createEnabledApps(). Your app subclass must have a public nested class
    /// called "AppFactory", inheriting publicly from simdb::AppFactoryBase.
    template <typename AppT, typename... Args>
    static void parameterizeAppFactoryInstance(size_t instance_num, [[maybe_unused]] Args&&... args)
    {
        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            auto factory = getAppFactory_<AppT>();
            factory->parameterize(instance_num, std::forward<Args>(args)...);
        }
        else
        {
            throw DBException("No nested class 'AppFactory' exists for app '")
                << AppT::NAME << "'.";
        }
    }

    /// AppManagers are associated 1-to-1 with a DatabaseManager.
    AppManager(DatabaseManager* db_mgr, std::ostream* msg_log = &std::cout, std::ostream* err_log = &std::cerr)
        : db_mgr_(db_mgr)
        , msg_log_(msg_log)
        , err_log_(err_log)
    {}

    /// Disable all messages to stdout.
    void disableMessageLog()
    {
        msg_log_.disable();
    }

    /// Disable all messages to stderr.
    void disableErrorLog()
    {
        err_log_.disable();
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
    void enableApp(const std::string& app_name, size_t num_instances = 1)
    {
        auto& app_factories = getAppFactories_();

        auto it = app_factories.find(app_name);
        if (it == app_factories.end())
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
                app_factories[instance_name] = factory;
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

        size_t instantiated_count = 0;
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

    /// See how many instances of a particular app are enabled.
    size_t getEnabledAppInstances(const std::string& app_name)
    {
        return enabled(app_name) ? enabled_apps_.at(app_name) : 0;
    }

    /// See how many instances of a particular app are enabled.
    template <typename AppT>
    size_t getEnabledAppInstances()
    {
        return getEnabledAppInstances(AppT::NAME);
    }

    /// Call after command line args and config files are parsed.
    void createEnabledApps()
    {
        auto& app_factories = getAppFactories_();

        for (const auto& [app_name, num_instances] : enabled_apps_)
        {
            if (num_instances == 1)
            {
                App* app = app_factories[app_name]->createApp(db_mgr_, 0);
                // Single-instance apps have instance number 0 already
                apps_[app_name] = std::unique_ptr<App>(app);
            } else {
                for (size_t instance_num = 1; instance_num <= num_instances; ++instance_num)
                {
                    App* app = app_factories[app_name]->createApp(db_mgr_, instance_num);
                    app->setInstance(instance_num);
                    std::string instance_name = app_name + std::string("-") + std::to_string(instance_num);
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

        auto& app_factories = getAppFactories_();

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
                    auto& factory = app_factories[app_name];
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

    /// Call this once after postInit(). This will create all pipelines
    /// for all enabled apps, but it will not open the threads yet.
    void initializePipelines()
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

        // Print final pipeline configurations.
        msg_log_ << "\nSimDB app pipeline configuration for database '" << db_mgr_->getDatabaseFilePath() << "':\n";
        for (auto pipeline : pipeline_mgr_->getPipelines())
        {
            msg_log_ << "---- Pipeline: " << pipeline->getName() << "\n";
            for (auto& [stage_name, stage] : pipeline->getOrderedStages())
            {
                msg_log_ << "------ Stage: " << stage_name << "\n";
            }
        }

        msg_log_ << std::endl;
    }

    /// Optionally call this method after initializePipelines(), but before
    /// openPipelines(). This will reduce the number of non-database threads
    /// to the minimum across all app pipelines.
    ///
    /// Note that you can either call minimizeThreads() OR minimizeThreads(app1, app2, ...)
    /// but you cannot call both.
    void minimizeThreads()
    {
        if (!pipeline_mgr_)
        {
            throw DBException("Pipeline manager not set - did you call initializePipelines()?");
        }
        pipeline_mgr_->minimizeThreads();
    }

    /// Optionally call this method after initializePipelines(), but before
    /// openPipelines(). This will share the minimum number of non-database
    /// threads across the given apps' pipelines.
    template <typename... Apps>
    void minimizeThreads(const App* app, Apps&&... rest)
    {
        if (!pipeline_mgr_)
        {
            throw DBException("Pipeline manager not set - did you call initializePipelines()?");
        }
        pipeline_mgr_->minimizeThreads(app, std::forward<Apps>(rest)...);
    }

    /// Call this once after initializePipelines() (and after minimizeThreads()
    /// if you called that too).
    void openPipelines()
    {
        PROFILE_APP_PHASE

        if (!pipeline_mgr_)
        {
            throw DBException("Pipeline manager not set - did you call initializePipelines()?");
        }
        pipeline_mgr_->openPipelines();
    }

    /// This method is to be called after the main simulation loop ends.
    /// All running apps' pipelines will be flushed, and all threads
    /// will be torn down.
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

    /// Get a static map for all registered app factories.
    static std::map<std::string, std::shared_ptr<AppFactoryBase>> & getAppFactories_()
    {
        static std::map<std::string, std::shared_ptr<AppFactoryBase>> app_factories;
        return app_factories;
    }

    /// Access an app factory.
    template <typename AppT>
    static
    std::conditional_t<
        utils::has_nested_factory<AppT>::value,
        typename AppT::AppFactory*,
        AppFactory<AppT>*>
    getAppFactory_(bool must_exist = true)
    {
        auto& app_factories = getAppFactories_();

        auto it = app_factories.find(AppT::NAME);
        if (it == app_factories.end())
        {
            if (must_exist)
            {
                throw DBException("No factory named ") << AppT::NAME << " exists.";
            }
            return nullptr;
        }

        using factory_t = std::conditional_t<
            utils::has_nested_factory<AppT>::value,
            typename AppT::AppFactory,
            AppFactory<AppT>>;

        auto factory = dynamic_cast<factory_t*>(it->second.get());
        if (!factory)
        {
            throw DBException("Factory is not the expected subclass type");
        }

        return factory;
    }

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
            if (msg_out_)
            {
                *msg_out_ << "SimDB: Entering " << block_name << "\n";
            }
        }

        ~ScopedTimer()
        {
            if (!msg_out_)
            {
                return;
            }

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
            if (out_ && enabled_)
            {
                *out_ << msg;
                out_->flush();
            }
            return *this;
        }

        Logger& operator<<(const char* msg)
        {
            if (out_ && enabled_)
            {
                *out_ << msg;
                out_->flush();
            }
            return *this;
        }

        Logger& operator<<(std::ostream& (*manip)(std::ostream&))
        {
            if (out_ && enabled_)
            {
                manip(*out_);
                out_->flush();
            }
            return *this;
        }

        operator std::ostream*()
        {
            return enabled_ ? out_ : nullptr;
        }

        void disable()
        {
            enabled_ = false;
        }

        void enable()
        {
            enabled_ = true;
        }

    private:
        std::ostream* out_ = nullptr;
        bool enabled_ = true;
    };

    Logger msg_log_;
    Logger err_log_;
};

} // namespace simdb
