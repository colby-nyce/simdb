#pragma once

#include "simdb/Exceptions.hpp"

/// Aside from its core SQLite functionality, SimDB provides a framework for
/// creating "apps" which get selectively enabled based on your simulation
/// configuration (e.g. command line options, config file, etc).
///
///   - Create your own data/metadata schema tables just for your app
///   - Use SimDB utilities to build async compression/DB pipelines
///
/// Example applications:
///
///   - Logger that records simulation events
///   - Profiler that tracks performance metrics
///   - Pipeline collector
///   - SQLite-to-HDF5 converter
///   - Backend for a live data visualization GUI / web interface
///
/// Since SimDB is designed to be simulator-agnostic, apps also provide
/// a variety of hooks that allow you to run code at different stages of
/// the simulation lifecycle and to ensure that all apps in your simulator
/// are initialized and run in a consistent manner.
///
///   - appendSchema:  first hook after command line args / config files are parsed
///   - preInit:       called before the simulation starts, after command line parsing
///   - preSim:        called before the simulation loop starts
///   - postSim:       called after the simulation loop ends
///   - teardown:      called after the simulation ends, for resource cleanup tasks
///
/// The general paradigm is that your simulator has a single output database,
/// with 1-to-many apps that are all writing to it with their own custom schemas
/// and logic.

namespace simdb
{

class DatabaseManager;

/// Base class for SimDB applications. Note that app subclasses are given
/// the DatabaseManager instance as a constructor argument, so they can
/// access the database and perform operations like appending schemas,
/// inserting records, etc.)
class App
{
public:
    virtual ~App() = default;
    virtual void appendSchema() {}
    virtual void preInit(int argc, char** argv) { (void)argc; (void)argv; }
    virtual void preSim() {}
    virtual void postSim() {}
    virtual void teardown() {}

protected:
    int getAppID_() const { return app_id_; }

    App *const __this__ = this;

private:
    int app_id_ = 0;

    // Allow AppManager to set the app ID
    friend class AppManager;
};

class AppFactoryBase
{
public:
    virtual ~AppFactoryBase() = default;
    virtual void setNumCompressionThreads(size_t num_threads) = 0;
    virtual App* createApp(DatabaseManager* db_mgr) = 0;
};

template <typename AppT>
class AppFactory : public AppFactoryBase
{
public:
    void setNumCompressionThreads(size_t num_threads) override
    {
        if (created_)
        {
            throw DBException("Cannot change number of compression threads after app creation.");
        }
        num_compression_threads_ = num_threads;
    }

    App* createApp(DatabaseManager* db_mgr) override
    {
        created_ = true;
        return new AppT(db_mgr, num_compression_threads_);
    }

private:
    size_t num_compression_threads_ = 0;
    bool created_ = false;
};

} // namespace simdb
