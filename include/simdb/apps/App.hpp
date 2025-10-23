// <App.hpp> -*- C++ -*-

#pragma once

#include <memory>
#include <vector>

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
///   - postInit:      called before the simulation starts, after command line parsing
///   - postSim:       called after the simulation loop ends
///   - teardown:      called after the simulation ends, for resource cleanup tasks
///
/// The general paradigm is that your simulator has a single output database,
/// with 1-to-many apps that are all writing to it with their own custom schemas
/// and logic.

namespace simdb {

class DatabaseManager;
class Schema;

namespace pipeline {
    class AsyncDatabaseAccessor;
    class PipelineManager;
}

/// Base class for SimDB applications. Note that app subclasses are given
/// the DatabaseManager instance as a constructor argument, so they can
/// access the database and perform operations like appending schemas,
/// inserting records, etc.)
class App
{
public:
    virtual ~App() = default;
    void setInstance(size_t instance) { instance_ = instance; }
    size_t getInstance() const { return instance_; }
    virtual void postInit(int argc, char** argv) { (void)argc; (void)argv; }
    virtual void createPipeline(pipeline::PipelineManager*) {}
    virtual void preTeardown() {}
    virtual void postTeardown() {}

private:
    size_t instance_ = 1;
};

class AppFactoryBase
{
public:
    virtual ~AppFactoryBase() = default;
    virtual App* createApp(DatabaseManager*, size_t instance_num = 0) = 0;
    virtual void defineSchema(Schema& schema) const = 0;
};

template <typename AppT>
class AppFactory : public AppFactoryBase
{
public:
    App* createApp(DatabaseManager* db_mgr, size_t instance_num = 0) override
    {
        (void)instance_num;
        return new AppT(db_mgr);
    }

    void defineSchema(Schema& schema) const override
    {
        AppT::defineSchema(schema);
    }
};

} // namespace simdb
