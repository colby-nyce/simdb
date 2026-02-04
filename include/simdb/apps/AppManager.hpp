// <AppManager.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/pipeline/PipelineManager.hpp"

#include <map>
#include <set>
#include <iostream>

#define PROFILE_APP_PHASE [[maybe_unused]] ScopedTimer timer(getDatabaseManager(), __FUNCTION__, msg_log_);

namespace simdb {

namespace utils {
    template <typename, typename = void>
    struct has_nested_factory : std::false_type{};

    template <typename T>
    struct has_nested_factory<T, std::void_t<typename T::AppFactory>> : std::true_type{};
}

class AppManagers;

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
            auto & app_factories = getDefaultAppFactories_();
            if (app_factories.find(AppT::NAME) != app_factories.end())
            {
                throw DBException("App already registered: ") << AppT::NAME;
            }

            constexpr size_t global_instance_num = 0;
            auto & factory = app_factories[AppT::NAME][global_instance_num];
            factory = std::make_shared<AppFactory<AppT>>();
        }
    }

    /// Get our associated DatabaseManager.
    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
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
    ///   // Parameterize before createEnabledApps()
    ///
    ///   // How to parameterize all instances of your app:
    ///   app_mgr.parameterizeAppFactory<MyApp>(1 /*x*/, 2.2 /*y*/);
    ///
    ///   // How to parameterize one instance of your app:
    ///   app_mgr.parameterizeAppFactoryInstance<MyApp>(1 /*inst num*/, 1 /*x*/, 2.2 /*y*/);
    ///   app_mgr.parameterizeAppFactoryInstance<MyApp>(2 /*inst num*/, 3 /*x*/, 4.4 /*y*/);
    ///
    ///   // Assume have the AppManager by now:
    ///   app_mgr.enableApp(MyApp::NAME);
    ///   app_mgr.enableApp(MyApp::NAME, 2 /*inst count*/);
    ///
    ///   // Then continue and create the apps:
    ///   app_mgr.createEnabledApps();
    ///
    /// IMPORTANT: If you already configured specific app instance factories
    /// with parameterizeAppFactoryInstance(), this method will throw away
    /// all your factory configurations, and all instances will end up using
    /// the same factory that is being "globally" configured right now (unless
    /// you call parameterizeAppFactoryInstance() again). Only a warning will
    /// be issued.
    template <typename AppT, typename... Args>
    void parameterizeAppFactory([[maybe_unused]] Args&&... args)
    {
        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            if (!enabled(AppT::NAME))
            {
                throw DBException("You need to call enableApp() before parameterizing factories.");
            }

            auto& app_factories = getDefaultAppFactories_();

            auto num_overwritten = app_factories[AppT::NAME].size();
            if (num_overwritten > 0)
            {
                std::cout << "WARNING: Throwing away " << num_overwritten
                    << " app factor" << (num_overwritten > 1 ? "ies " : "y")
                    << " and creating a new one for all apps of type "
                    << AppT::NAME << ". Was this intentional?\n";
            }

            app_factories[AppT::NAME].clear();

            constexpr size_t global_instance_num = 0;
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
    void parameterizeAppFactoryInstance(size_t instance_num, [[maybe_unused]] Args&&... args)
    {
        if constexpr (utils::has_nested_factory<AppT>::value)
        {
            if (!enabled(AppT::NAME))
            {
                throw DBException("You need to call enableApp() before parameterizing factories.");
            }

            std::cout << "Parameterizing '" << AppT::NAME << "' app, instance " << instance_num << "\n";
            auto factory = getNestedAppFactory_<AppT>(instance_num);
            factory->parameterize(std::forward<Args>(args)...);
        }
        else
        {
            throw DBException("No nested class 'AppFactory' exists for app '")
                << AppT::NAME << "'.";
        }
    }

    /// Disable all logged messages.
    void disableMessageLog()
    {
        msg_log_.disable();
    }

    /// Disable all logged errors.
    void disableErrorLog()
    {
        err_log_.disable();
    }

    /// Redirect messages (defaults to stdout)
    void redirectMessageLog(std::ostream* msg_log)
    {
        msg_log_.redirectMessages(msg_log);
    }

    /// Redirect errors (defaults to stderr)
    void redirectErrorsLog(std::ostream* err_log)
    {
        err_log_.redirectErrors(err_log);
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
        getEnabledApps_()[app_name] = num_instances;
    }

    template <typename AppT>
    void enableApp(size_t num_instances = 1)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        enableApp(AppT::NAME, num_instances);
    }

    /// Check if your app is enabled (might not be instantiated yet).
    bool enabled(const std::string& app_name)
    {
        const auto& enabled_apps = getEnabledApps_();
        return enabled_apps.find(app_name) != enabled_apps.end();
    }

    /// Check if your app is enabled (might not be instantiated yet).
    template <typename AppT>
    bool enabled()
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");
        return enabled(AppT::NAME);
    }

    /// See how many instances of a particular app are enabled.
    size_t getEnabledAppInstances(const std::string& app_name)
    {
        return enabled(app_name) ? getEnabledApps_().at(app_name) : 0;
    }

    /// See how many instances of a particular app are enabled.
    template <typename AppT>
    size_t getEnabledAppInstances()
    {
        return getEnabledAppInstances(AppT::NAME);
    }

    /// Get an instantiated app. You may only call this method if AppT had exactly one
    /// instance configured:
    ///   AppManager::enableApp(AppT::NAME);
    ///   AppManager::enableApp(AppT::NAME, 1);
    ///
    /// For multi-instance apps, call getAppInstance<AppT>(instance_num) (zero-based).
    /// Note that for single-instance apps, you can still call getAppInstance<AppT>(0).
    template <typename AppT>
    AppT* getApp()
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");

        if (!enabled<AppT>())
        {
            return nullptr;
        }

        auto& enabled_apps = getEnabledApps_();
        if (enabled_apps.at(AppT::NAME) > 1)
        {
            throw DBException("Need to call getAppInstance<AppT>(instance_num) since ")
                << "the '" << AppT::NAME << "' app was configured to have "
                << enabled_apps.at(AppT::NAME) << " instances.";
        }

        // Look for instance name
        std::string instance_name = AppT::NAME + std::string("-0");
        auto it = apps_.find(instance_name);
        if (it != apps_.end())
        {
            auto app = dynamic_cast<AppT*>(it->second.get());
            if (!app)
            {
                throw DBException("App of type ") << AppT::NAME << " is not of type " << typeid(AppT).name();
            }
            return app;
        }

        return nullptr;
    }

    /// Get an instantiated app. Call this method for multi-instance apps.
    template <typename AppT>
    AppT* getAppInstance(size_t instance_num)
    {
        static_assert(std::is_base_of<App, AppT>::value, "AppT must derive from App");

        if (!enabled<AppT>())
        {
            return nullptr;
        }

        // Look for instance name
        std::string instance_name = AppT::NAME + std::string("-") + std::to_string(instance_num);
        auto it = apps_.find(instance_name);
        if (it != apps_.end())
        {
            auto app = dynamic_cast<AppT*>(it->second.get());
            if (!app)
            {
                throw DBException("App of type ") << AppT::NAME << " is not of type " << typeid(AppT).name();
            }
            return app;
        }

        return nullptr;
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

private:
    /// AppManagers are associated 1-to-1 with a DatabaseManager.
    AppManager(DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
        , msg_log_(&std::cout)
        , err_log_(&std::cerr)
    {}

    /// AppManager only to be instantiated by simdb::AppManagers
    friend class AppManagers;

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

    /// Call after command line args and config files are parsed.
    void createEnabledApps_()
    {
        for (const auto& [app_name, num_instances] : getEnabledApps_())
        {
            for (size_t instance_num = 0; instance_num < num_instances; ++instance_num)
            {
                auto factory = getAppFactory_(app_name, instance_num, false /*do not create*/);
                if (!factory)
                {
                    continue;
                }

                App* app = factory->createApp(db_mgr_);
                app->setInstance(instance_num);
                std::string instance_name = app_name + std::string("-") + std::to_string(instance_num);
                apps_[instance_name] = std::unique_ptr<App>(app);
            }
        }
    }

    /// Create app-specific schemas for the instantiated apps.
    void createSchemas_()
    {
        PROFILE_APP_PHASE

        db_mgr_->safeTransaction(
            [&]()
            {
                for (const auto& [app_name, app] : apps_)
                {
                    auto user_app_name = app_name;
                    auto idx = user_app_name.find_last_of("-");
                    assert(idx != std::string::npos);
                    user_app_name = user_app_name.substr(0, idx);

                    auto tmp_substr = app_name.substr(idx);
                    auto instance_num = static_cast<size_t>(std::stoull(tmp_substr));
                    AppFactoryBase* factory = getAppFactory_(user_app_name, instance_num);

                    Schema app_schema;
                    factory->defineSchema(app_schema);
                    db_mgr_->appendSchema(app_schema);
                }
            });
    }

    /// Call this after command line args and config files are parsed.
    void postInit_(int argc, char** argv)
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
    void initializePipelines_()
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

    /// Call this once after initializePipelines() (and after minimizeThreads()
    /// if you called that too).
    void openPipelines_()
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
    void postSimLoopTeardown_()
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
    }

    /// Delete all instantiated apps. This may be needed since AppManager
    /// is a singleton and your simulator might want to call app destructors
    /// before the AppManager itself is destroyed on program exit.
    void destroyAllApps_()
    {
        pipeline_mgr_.reset();
        apps_.clear();
    }

    using app_factories_t =
        std::map<std::string,                           // App name
            std::map<size_t,                            // App instance
                     std::shared_ptr<AppFactoryBase>>>; // Factory

    /// Get a static map for all registered app factories.
    static app_factories_t & getDefaultAppFactories_()
    {
        static app_factories_t app_factories;
        return app_factories;
    }

    /// Non-static app factories created by this AppManager.
    /// Specific to apps that have a nested AppFactory class.
    app_factories_t nested_app_factories_;

    /// Access an app factory.
    template <typename AppT>
    typename AppT::AppFactory*
    getNestedAppFactory_(size_t instance_num, bool create_if_needed = true)
    {
        if (!create_if_needed)
        {
            auto it = nested_app_factories_.find(AppT::NAME);
            if (it == nested_app_factories_.end())
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

        auto& factory = nested_app_factories_[AppT::NAME][instance_num];
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
        auto app_factories = getAllAppFactories_();

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

    /// Return a union of the global and local app factories.
    app_factories_t getAllAppFactories_() const
    {
        auto default_app_factories = getDefaultAppFactories_();
        return combineFactories_(default_app_factories, nested_app_factories_);
    }

    /// Merge two app_factories_t together, verifying that they
    /// have no overlap.
    app_factories_t combineFactories_(
        const app_factories_t& global,
        const app_factories_t& local) const
    {
        app_factories_t result = global;

        for (const auto& [app_name, local_instances] : local) {
            auto& result_instances = result[app_name];

            for (const auto& [instance_id, factory] : local_instances) {
                const auto [it, inserted] =
                    result_instances.emplace(instance_id, factory);

                if (!inserted) {
                    throw std::logic_error(
                        "Duplicate AppFactory entry for app '" + app_name +
                        "', instance " + std::to_string(instance_id));
                }
            }
        }

        return result;
    }

    /// Instantiated apps (implicitly enabled).
    /// Key is the App's NAME static member, or NAME-<instance>
    /// for apps with multiple instances (one-based).
    std::map<std::string, std::unique_ptr<App>> apps_;

    /// Enabled apps (may or may not be instantiated).
    /// Key is the App's NAME static member.
    /// Value is the number of instances to create.
    static std::map<std::string, size_t> & getEnabledApps_()
    {
        static std::map<std::string, size_t> enabled_apps;
        return enabled_apps;
    }

    /// Associated database.
    DatabaseManager* db_mgr_ = nullptr;

    /// All pipelines and threads are managed by PipelineManager.
    std::unique_ptr<pipeline::PipelineManager> pipeline_mgr_;

    /// RAII timer to measure the performance of various app setup/teardown phases.
    class ScopedTimer
    {
    public:
        ScopedTimer(const DatabaseManager* db_mgr, const std::string& block_name, std::ostream* msg_out = &std::cout)
            : start_(std::chrono::high_resolution_clock::now())
            , block_name_(block_name)
            , msg_out_(msg_out)
        {
            if (msg_out_)
            {
                auto db_filepath = db_mgr->getDatabaseFilePath();
                *msg_out_ << "SimDB: Entering " << block_name << " for database: "
                          << db_filepath << "\n";
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

        void enable(bool enabled)
        {
            enabled_ = enabled;
        }

        void redirectMessages(std::ostream* msg_log)
        {
            out_ = msg_log;
            enable(out_ != nullptr);
        }

        void redirectErrors(std::ostream* err_log)
        {
            out_ = err_log;
            enable(out_ != nullptr);
        }

    private:
        std::ostream* out_ = nullptr;
        bool enabled_ = true;
    };

    Logger msg_log_;
    Logger err_log_;
};

/// This class holds onto all DatabaseManagers and their AppManagers.
class AppManagers
{
public:
    /// Get (or create) a new AppManager with a database filename / filepath
    AppManager& getAppManager(const std::string& db_file, bool create_if_needed = true)
    {
        auto& app_mgr = app_mgrs_by_db_file_[db_file];
        if (!app_mgr && !create_if_needed)
        {
            app_mgrs_by_db_file_.erase(db_file);
            throw DBException("AppManager does not exist for DB: ") << db_file;
        }
        else if (!app_mgr)
        {
            // Sanity check
            assert(db_mgrs_by_db_file_.find(db_file) == db_mgrs_by_db_file_.end());

            // Create a new DatabaseManager (if needed)
            std::shared_ptr<DatabaseManager> db_mgr;
            if (auto it = db_mgrs_by_db_file_.find(db_file); it != db_mgrs_by_db_file_.end())
            {
                db_mgr = it->second;
            }
            else
            {
                db_mgr = std::make_shared<DatabaseManager>(db_file, true /*new file*/);
            }

            // Create a new AppManager for this database
            app_mgr.reset(new AppManager(db_mgr.get()));

            db_mgrs_by_db_file_[db_file] = db_mgr;
            app_mgrs_by_db_mgr_[db_mgr.get()] = app_mgr;
            db_mgrs_by_app_mgr_[app_mgr.get()] = db_mgr;
        }
        return *app_mgr;
    }

    /// If there is only one AppManager, return it. Otherwise throw.
    AppManager& getAppManager()
    {
        if (app_mgrs_by_db_file_.size() == 1)
        {
            return *app_mgrs_by_db_file_.begin()->second;
        }

        throw DBException("Cannot call getAppManager() since there are ")
            << app_mgrs_by_db_file_.size() << " AppManager's. Must be only one.";
    }

    /// If there is only one AppManager, return its DatabaseManager. Otherwise throw.
    DatabaseManager& getDatabaseManager()
    {
        if (db_mgrs_by_db_file_.size() == 1)
        {
            return *db_mgrs_by_db_file_.begin()->second;
        }

        throw DBException("Cannot call getDatabaseManager() since there are ")
            << db_mgrs_by_db_file_.size() << " AppManager's. Must be only one.";
    }

    /// Get a DatabaseManager for the given DB file. Throws if not found.
    DatabaseManager& getDatabaseManager(const std::string& db_file)
    {
        auto it = db_mgrs_by_db_file_.find(db_file);
        if (it == db_mgrs_by_db_file_.end())
        {
            throw DBException("DatabaseManager does not exist for file '")
                << db_file << "'.";
        }
        return *it->second;
    }

    /// Get a mapping from all active AppManager's and their associated
    /// DatabaseManager's.
    std::vector<std::pair<AppManager*, DatabaseManager*>> getAllManagers()
    {
        std::vector<std::pair<AppManager*, DatabaseManager*>> mgrs;
        for (auto& [db_file, db_mgr] : db_mgrs_by_db_file_)
        {
            auto it = app_mgrs_by_db_file_.find(db_file);
            if (it == app_mgrs_by_db_file_.end())
            {
                continue;
            }

            auto& app_mgr = it->second;
            mgrs.push_back(std::make_pair(app_mgr.get(), db_mgr.get()));
        }
        return mgrs;
    }

    /// Call after command line args and config files are parsed.
    void createEnabledApps()
    {
        for (auto& [app_mgr, _] : getAllManagers())
        {
            app_mgr->createEnabledApps_();
        }
    }

    /// Create app-specific schemas for the instantiated apps.
    void createSchemas()
    {
        for (auto& [app_mgr, _] : getAllManagers())
        {
            app_mgr->createSchemas_();
        }
    }

    /// Call this after command line args and config files are parsed.
    void postInit(int argc, char** argv)
    {
        for (auto& [app_mgr, _] : getAllManagers())
        {
            app_mgr->postInit_(argc, argv);
        }
    }

    /// Call this once after postInit(). This will create all pipelines
    /// for all enabled apps, but it will not open the threads yet.
    void initializePipelines()
    {
        for (auto& [app_mgr, _] : getAllManagers())
        {
            app_mgr->initializePipelines_();
        }
    }

    /// Call this once after initializePipelines() (and after minimizeThreads()
    /// if you called that too).
    void openPipelines()
    {
        for (auto& [app_mgr, _] : getAllManagers())
        {
            app_mgr->openPipelines_();
        }
    }

    /// Call postSimLoopTeardown() on all AppManager's. This destroys all Apps
    /// and AppManager's. It does NOT destroy the DatabaseManager's. Once this
    /// method is called, you have to access the DatabaseManager's using the
    /// 'getDatabaseManager(db_file)' API.
    void postSimLoopTeardown()
    {
        for (auto& [_, app_mgr] : app_mgrs_by_db_file_)
        {
            app_mgr->postSimLoopTeardown_();
        }

        destroy_(false);
    }

    /// Destroy every AppManager and DatabaseManager we have.
    void destroyAll()
    {
        destroy_(true);
    }

private:
    void destroy_(bool destroy_all)
    {
        /// Destroy all AppManagers, DatabaseManagers, and Apps
        for (auto& [_, app_mgr] : app_mgrs_by_db_file_)
        {
            app_mgr->destroyAllApps_();
        }

        // Destroy AppManager's first in case they need to touch
        // the database in their destructors.
        app_mgrs_by_db_mgr_.clear();
        app_mgrs_by_db_file_.clear();

        // Clear DatabaseManager maps. Hang onto db_mgrs_by_db_file_
        // so 'getDatabaseManager(db_file)' is still available.
        db_mgrs_by_app_mgr_.clear();

        // Destroy DatabaseManagers
        if (destroy_all)
        {
            db_mgrs_by_db_file_.clear();
        }
    }

    std::map<std::string, std::shared_ptr<DatabaseManager>> db_mgrs_by_db_file_;
    std::map<std::string, std::shared_ptr<AppManager>> app_mgrs_by_db_file_;
    std::map<DatabaseManager*, std::shared_ptr<AppManager>> app_mgrs_by_db_mgr_;
    std::map<AppManager*, std::shared_ptr<DatabaseManager>> db_mgrs_by_app_mgr_;
};

} // namespace simdb
