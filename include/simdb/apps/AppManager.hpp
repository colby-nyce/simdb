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
        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            // We don't need to create the nested factory object now.
            // When apps provide a nested factory, they are supposed
            // to call parameterizeApp<T>() / parameterizeAppInstance<T>()
            // before calling createEnabledApps().
            //
            // If they forget to parameterize their own custom-factory app,
            // we will know it since we won't have the factory. These factories
            // are created in the parameterize*() calls.
            return;
        }
        else
        {
            auto & app_factories = getAppFactories_();
            if (app_factories.find(AppT::NAME) != app_factories.end())
            {
                throw DBException("App already registered: ") << AppT::NAME;
            }

            constexpr size_t global_instance_num = 0;
            auto & factory = app_factories[AppT::NAME][global_instance_num];
            factory = std::make_shared<AppFactory<AppT>>();
        }
    }

    /// Parameterize an app factory. Call this before createEnabledApps().
    /// Your app subclass must have a public nested class called "AppFactory",
    /// inheriting publicly from simdb::AppFactoryBase. Your nested AppFactory
    /// can have any signature it needs to accept all required app constructor
    /// arguments.
    ///
    ///   void parameterize(int x, float y)
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
    ///           void parameterize(int x, float y)
    ///           {
    ///               ctor_args_ = std::make_pair(x, y);
    ///           }
    ///
    ///           simdb::App* createApp(simdb::DatabaseManager* db_mgr) override
    ///           {
    ///               const auto & [x, y] = ctor_args_;
    ///               return new MyApp(db_mgr, x, y);
    ///           }
    ///
    ///           void defineSchema(simdb::Schema& schema) const override
    ///           {
    ///               ...
    ///           }
    ///
    ///       private:
    ///           std::pair<int,float> ctor_args_;
    ///       };
    ///   };
    ///
    /// Then do this:
    ///
    ///   // Parameterize before createEnabledApps() - or even before creating AppManager.
    ///
    ///   // How to parameterize all instances of your app:
    ///   AppManager::parameterizeAppFactory<MyApp>(1 /*x*/, 2.2 /*y*/);
    ///
    ///   // How to parameterize one instance of your app:
    ///   AppManager::parameterizeAppFactoryInstance<MyApp>(1 /*inst num*/, 1 /*x*/, 2.2 /*y*/);
    ///   AppManager::parameterizeAppFactoryInstance<MyApp>(2 /*inst num*/, 3 /*x*/, 4.4 /*y*/);
    ///
    ///   // Assume have the AppManager by now:
    ///   app_mgr.enableApp(MyApp::NAME);
    ///   app_mgr.enableApp(MyApp::NAME, 2 /*inst count*/);
    ///
    ///   // Then continue and create the apps:
    ///   app_mgr.createEnabledApps();
    template <typename AppT, typename... Args>
    static void parameterizeAppFactory([[maybe_unused]] Args&&... args)
    {
        constexpr size_t global_instance_num = 0;
        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            auto factory = getNestedAppFactory_<AppT>(global_instance_num);
            factory->parameterize(std::forward<Args>(args)...);
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
        if (instance_num == 0)
        {
            // This is likely user error as this API is meant for parameterizing
            // specific instances. Using a value of 0 means "do this for all apps
            // of this type", and the parameterizeAppFactory() method should be
            // called instead.
            throw DBException("Cannot call parameterizeAppFactoryInstance() with ")
                << "an instance number of 0. Call parameterizeAppFactory() instead.";
        }

        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            auto factory = getNestedAppFactory_<AppT>(instance_num);
            factory->parameterize(std::forward<Args>(args)...);
        }
        else
        {
            throw DBException("No nested class 'AppFactory' exists for app '")
                << AppT::NAME << "'.";
        }
    }

    /// Check if we have a factory for the app with the given name.
    /// If you are checking for a factory for an app that has a nested
    /// AppFactory class in it, consider using hasAppFactoryOfType<AppT>().
    static bool hasAppFactory(const std::string& app_name)
    {
        const auto& app_factories = getAppFactories_();
        if (app_factories.find(app_name) == app_factories.end())
        {
            return false;
        }
        return true;
    }

    /// Check if we have a factory for the app with the given name and type.
    /// Only call this if your app is expected to have only ONE instance.
    /// For apps with >1 instance, call hasAppFactoryInstanceOfType<AppT>().
    template <typename AppT>
    static bool hasAppFactoryOfType()
    {
        static_assert(utils::has_nested_factory<AppT>::value,
                      "You may only call this method for apps with a nested AppFactory");

        constexpr size_t global_instance_num = 0;
        return getNestedAppFactory_<AppT>(global_instance_num, false /*do not create*/) != nullptr;
    }

    /// Check if we have a factory for the app with the given name, type, and
    /// instance number. For apps with only one instance, you should call the
    /// hasAppFactoryOfType<AppT>() method instead.
    template <typename AppT>
    static bool hasAppFactoryInstanceOfType(size_t instance_num)
    {
        static_assert(utils::has_nested_factory<AppT>::value,
                      "You may only call this method for apps with a nested AppFactory");

        if (instance_num == 0)
        {
            throw DBException("Call hasAppFactoryOfType<AppT>() instead for 1-instance apps");
        }

        return getNestedAppFactory_<AppT>(instance_num, false /*do not create*/) != nullptr;
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
        auto throw_no_factory = [](const std::string& app_name)
        {
            throw DBException("No factory exists for app ")
                << app_name << ". Remember that all apps must be "
                << "registered with REGISTER_SIMDB_APPLICATION. "
                << "If your app has a nested AppFactory class, "
                << "you must call one of the parameterizeApp*() "
                << "APIs prior to calling createEnabledApps().";
        };

        for (const auto& [app_name, num_instances] : enabled_apps_)
        {
            if (num_instances == 1)
            {
                constexpr size_t instance_num = 0;
                auto factory = getAppFactory_(app_name, instance_num);
                if (!factory)
                {
                    throw_no_factory(app_name);
                }

                App* app = factory->createApp(db_mgr_);
                // Single-instance apps have instance number 0 already
                apps_[app_name] = std::unique_ptr<App>(app);
            } else {
                for (size_t instance_num = 1; instance_num <= num_instances; ++instance_num)
                {
                    auto factory = getAppFactory_(app_name, instance_num);
                    if (!factory)
                    {
                        throw_no_factory(app_name);
                    }

                    App* app = factory->createApp(db_mgr_);
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
                    AppFactoryBase* factory = nullptr;
                    for (size_t instance_num = 0; instance_num < getEnabledAppInstances(app_name); ++instance_num)
                    {
                        constexpr bool must_exist = false;
                        factory = getAppFactory_(app_name, instance_num, must_exist);
                        if (factory)
                        {
                            break;
                        }
                    }

                    // If we did not find the factory, it is likely due to having an
                    // app with a non-zero number of requested instances. In that case,
                    // the app_name here will be something like "simple-pipeline-3",
                    // while the factory is keyed off of "simple-pipeline".
                    if (!factory)
                    {
                        auto loc_app_name = app_name;
                        auto idx = loc_app_name.find_last_of("-");
                        if (idx != std::string::npos)
                        {
                            loc_app_name = loc_app_name.substr(0, idx);
                            for (size_t instance_num = 0; instance_num < getEnabledAppInstances(loc_app_name); ++instance_num)
                            {
                                constexpr bool must_exist = false;
                                factory = getAppFactory_(loc_app_name, instance_num, must_exist);
                                if (factory)
                                {
                                    break;
                                }
                            }
                        }
                    }

                    // Throw if we did not find the factory
                    if (!factory)
                    {
                        throw DBException("Factory does not exist for app: ") << app_name;
                    }

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

    using app_factories_t =
        std::map<std::string,                           // App name
            std::map<size_t,                            // App instance
                     std::shared_ptr<AppFactoryBase>>>; // Factory

    /// Get a static map for all registered app factories.
    static app_factories_t & getAppFactories_()
    {
        static app_factories_t app_factories;
        return app_factories;
    }

    /// Access an app factory.
    template <typename AppT>
    static typename AppT::AppFactory*
    getNestedAppFactory_(size_t instance_num, bool create_if_needed = true)
    {
        auto& app_factories = getAppFactories_();
        if (!create_if_needed)
        {
            auto it = app_factories.find(AppT::NAME);
            if (it == app_factories.end())
            {
                return nullptr;
            }

            auto it2 = it->second.find(instance_num);
            if (it2 == it->second.end())
            {
                return nullptr;
            }

            auto factory = dynamic_cast<typename AppT::AppFactory*>(it2->second.get());
            if (!factory)
            {
                throw DBException("Failed to downcast app factory for '")
                    << AppT::NAME << "'.";
            }

            return factory;
        }

        auto& factory = app_factories[AppT::NAME][instance_num];
        if (!factory)
        {
            factory = std::make_shared<typename AppT::AppFactory>();
        }

        return dynamic_cast<typename AppT::AppFactory*>(factory.get());
    }

    AppFactoryBase* getAppFactory_(const std::string& app_name,
                                   size_t instance_num,
                                   bool must_exist = true) const
    {
        auto& app_factories = getAppFactories_();

        auto it = app_factories.find(app_name);
        if (it == app_factories.end())
        {
            if (must_exist)
            {
                throw DBException("No factory exists for app: ") << app_name;
            }
            return nullptr;
        }

        auto it2 = it->second.find(instance_num);
        if (it2 == it->second.end())
        {
            // If we looked for a specific app instance factory (non-zero)
            // and could not find it, look for a "global" factory with
            // instance_num 0 for this app.
            if (instance_num > 0)
            {
                constexpr size_t global_instance_num = 0;
                return getAppFactory_(app_name, global_instance_num, must_exist);
            }

            if (must_exist)
            {
                throw DBException("No factory exists for instance ")
                    << instance_num << " for app: " << app_name;
            }
            return nullptr;
        }

        return it2->second.get();
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
