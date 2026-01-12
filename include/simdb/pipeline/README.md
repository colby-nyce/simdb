# Pipeline Basics

## Stages

A pipeline stage is composed of I/O ports with specific data types. All combinations of inputs and outputs are allowed (multi-input, multi-output, zero-input, zero-output).

A stage runs on its own thread, processing data in a greedy fashion until it tells SimDB that it had nothing left to do. Then the thread sleeps for a small amount of time (100ms by default), wakes back up and continues invoking the stage until told to sleep again.

SimDB provides the Stage class as well as the DatabaseStage class. When you create a Stage subclass, it runs on its own thread. However, when you subclass from the DatabaseStage class, it is put on a dedicated database thread with all other running pipelines' DatabaseStages. It is designed this way since multi-threaded access to the filesystem (SQLite) is never performant enough for high-volume data processing for fast C++ simulations.

Unlike Stage subclasses, the DatabaseStage subclasses will have access to the SQLite database (DatabaseManager) as well as having an API which provides more performant INSERTs using SQLite prepared statements.

## Ports

As noted above, stages are composed of I/O ports with specific data types. SimDB provides the ConcurrentQueue utility class which simply combines a FIFO queue API with a mutex. These queues sit between concurrently-running stages' I/O ports.

## Code Snippet: Non-Database Stage

Here is a simple Stage subclass which performs zlib compression on incoming data:

```
struct StatsValues {
    std::string uuid;
    std::vector<double> values;
};

struct ZlibStatsValues {
    std::string uuid;
    std::vector<char> bytes;
};

class ZlibStage : public simdb::pipeline::Stage
{
private:
    // This stage needs an input of uncompressed values:
    simdb::ConcurrentQueue<StatsValues>* input_queue_ = nullptr;

    // And after compression, it will need an output for the compressed values:
    simdb::ConcurrentQueue<ZlibStatsValues>* output_queue_ = nullptr;

public:
    ZlibStage()
    {
        // The addInPort_/addOutPort_() methods use pointer references so that SimDB
        // can assign our I/O ports' simdb::ConcurrentQueue's.
        addInPort_<StatsValues>("uncompressed_input", input_queue_);
        addOutPort_<ZlibStatsValues>("compressed_output", output_queue_);
    }

private:
    simdb::pipeline::PipelineAction run_(bool force) override
    {
        // The 'force' flag is true during flush events. Some stages might buffer
        // data internally e.g. in a std::vector, typically only sending whole
        // buffers to the next stage, but might need to send partial buffers when
        // flushing.
        //
        // Flush events occur when using pipeline flushers (discussed below) and
        // when simulation is over (right after tearing down pipeline threads).
        //
        // This zlib compressor has no internal buffer/storage, so we don't need
        // to consider this flag for this example stage.

        // If we can read a vector<double> input, compress it and send to the next
        // stage as a vector<char>.
        StatsValues uncompressed;
        if (input_queue_->try_pop(uncompressed))
        {
            ZlibStatsValues compressed;
            compressed.uuid = uncompressed.uuid;

            // #include "simdb/utils/Compress.hpp"
            simdb::compressData(uncompressed.values, compressed.bytes);

            // Send it along
            output_queue_->emplace(std::move(compressed));

            // Tell SimDB that we did something. It will continue greedily
            // processing this stage without sleeping.
            return simdb::pipeline::PipelineAction::PROCEED;
        }

        // We had no input data. Tell SimDB that we can sleep for a bit.
        return simdb::pipeline::PipelineAction::SLEEP;
    }
};
```

## Code Snippet: Database Stage

Here is a DatabaseStage which accepts the compressed bytes from the prior stage above,
and writes it to the database. For full transparency / clarity, some of the surrounding
simdb::App code is shown:

```
class StatsCollector : public simdb::App
{
public:
    // All SimDB apps provide this method to define schema tables
    static void defineSchema(simdb::Schema& schema)
    {
        using dt = simdb::SqlDataType;

        auto& stats_tbl = schema.addTable("CompressedStats");
        stats_tbl.addColumn("UUID", dt::string_t);
        stats_tbl.addColumn("StatsBlob", dt::blob_t);
    }

    ...

private:
    // Same stage as above
    class ZlibStage : public simdb::pipeline::Stage
    {
        ...
    };

    // DatabaseStage base class is templated on our App class type so that the stage
    // can use prepared statements for all of our app's schema tables as defined in
    // the defineSchema() method above.
    class DatabaseStage : public simdb::pipeline::DatabaseStage<StatsCollector>
    {
    private:
        // This stage needs an input of compressed values:
        simdb::ConcurrentQueue<ZlibStatsValues>* input_queue_ = nullptr;

        // For brevity, there is no output queue for this stage. But you can have
        // output queue(s) from database stages if you need them. An example pipeline
        // with DB stage outputs might be something like this:
        //
        //   Sim sends data -> Cache a copy of data -> Process data -> Database ----|
        //                             ^                                            |
        //                             |------------------- Evict ------------------|
        //
        // Where the cache holds onto copies of the original data for fast access,
        // and then evicts from the cache when the data has been written to the
        // database. This probably implies that the data packets are given some
        // sort of UUID, but the design is up to you.

    public:
        DatabaseStage()
        {
            addInPort_<ZlibStatsValues>("compressed_input", input_queue_);
        }

    private:
        simdb::pipeline::PipelineAction run_(bool force) override
        {
            // Read a compressed char buffer from the input queue
            ZlibStatsValues compressed;
            if (input_queue_->try_pop(compressed))
            {
                // Write to the database. We can do this two ways:
                //   1) Use DatabaseManager directly
                //   2) Use and reuse prepared statements for performance
                //
                // The first way is done like this:
                //
                //   auto db_mgr = getDatabaseManager_();
                //   db_mgr->INSERT(
                //       SQL_TABLE("CompressedStats"),
                //       SQL_COLUMNS("UUID", "StatsBlob"),
                //       SQL_VALUES(compressed.uuid, compressed.bytes));
                //
                // There is little reason to use DatabaseManager, so we
                // will typically use prepared statements:
                //
                auto inserter = getTableInserter_("CompressedStats");
                inserter->setColumnValue(0, compressed.uuid);
                inserter->setColumnValue(1, compressed.bytes);
                inserter->createRecord();

                // The reason DatabaseStage gives access to the DatabaseManager is to
                // support use cases where your database stage might want to run a query
                // on the database, delete some records, ..., basically anything besides
                // an INSERT.

                // Final note: All DatabaseStage::run_() methods are always implicitly
                // inside a batch BEGIN TRANSACTION / COMMIT TRANSACTION block. Multiple
                // other DB operations (insert, delete) might be inside this transaction,
                // whether from this stage, another DB stage in this app, or other DB stages
                // in other concurrently running SimDB apps. Batch transactions are performed
                // since they offer significantly higher performance and scalability over
                // smaller individual transactions.

                // Tell SimDB to continue greedily processing this stage.
                return simdb::pipeline::PipelineAction::PROCEED;
            }
        }

        // We had no input data. Tell SimDB that we can sleep for a bit.
        return simdb::pipeline::PipelineAction::SLEEP;
    };
}
```

## Code Snippet: Gluing It All Together

With your app's stages and ports defined, override the `createPipeline()` method
and connect everything together:

```
class StatsCollector : public simdb::App
{
private:
    // Input queue to the ZlibStage (pipeline "head").
    simdb::ConcurrentQueue<StatsValues>* pipeline_head_ = nullptr;

public:
    void createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr) override
    {
        // Give the pipeline a name and associate it with `this` app:
        auto pipeline = pipeline_mgr->createPipeline("stats-collector-pipeline", this);

        // Add stages and give them names.
        pipeline->addStage<ZlibStage>("compressor");
        pipeline->addStage<DatabaseStage>("db_writer");
        pipeline->noMoreStages();

        // Bind stage outputs to downstream stage inputs. Recall that these are
        // the port queues we have defined:
        //
        //     - compressor
        //        - uncompressed_input
        //        - compressed_output         -----|
        //     - db_writer                         | BIND
        //        - compressed_input          -----|
        //
        pipeline->bind("compressor.compressed_output", "db_writer.compressed_input");
        pipeline->noMoreBindings();

        // As soon as we call noMoreBindings(), all simdb::ConcurrentQueue's are assigned to
        // all stages. As of now, the "compressor.uncompressed_input" queue is not bound to
        // anything. Store the pipeline input queue:
        pipeline_head_ = pipeline->getInPortQueue<StatsValues>("compressor.uncompressed_input");

        // Note: There is no reason you cannot use multiple input ports for the pipeline's
        // first stage. For instance, your first stage might operate on tuples of values
        // that come into the pipeline at different times, thus the stage would have more
        // than one input queue, and will forward along / process the whole tuple when it
        // becomes available. In other words, such a pipeline would start out with a mux.
        // See examples/MultiPortStages/main.cpp
    }

    void process(StatsValues&& stats)
    {
        // Send the incoming stats values to the pipeline (move):
        pipeline_head_->emplace(std::move(stats));
    }

    void process(const StatsValues& stats)
    {
        // Send the incoming stats values to the pipeline (copy):
        pipeline_head_->push(stats);
    }

private:
    class ZlibStage ...
    class DatabaseStage ...
};
```

# Pipeline Extras

## Flushers

In the `createPipeline()` method, you can store pipeline flushers which will invoke your stage's `run_()` methods exhaustively until they all have nothing left to do (when they all return `PipelineAction::SLEEP`). The `run_()` methods will be invoked stage-by-stage in the order you provide in a round-robin fashion:

```
void StatsCollector::createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr)
{
    auto pipeline = pipeline_mgr->createPipeline(NAME, this);
    ...

    // std::unique_ptr<simdb::pipeline::Flusher> pipeline_flusher_;
    pipeline_flusher_ = pipeline->createFlusher({"compressor", "db_writer"});

    // Note that if you omit the explicit stages, the flusher will assume that it should
    // flush the stages in the same order given in the calls to addStage<T>(). In this
    // simple case, the following API does the same as above:
    // pipeline_flusher_ = pipeline->createFlusher();
}
```

You can then use the flusher inside APIs which require a synchronization point:

```
// Return the total number of stats records created
size_t StatsCollector::getNumCollected()
{
    // Flush everything in the "compressor" stage, then the "db_writer" stage:
    pipeline_flusher_->flush();

    // Run a query to count the total number of stats records created so far:
    auto query = db_mgr_->createQuery("CompressedStats");
    return query->count();
}
```

It is important to note that if any of the `createFlusher()` stages were `simdb::pipeline::DatabaseStage`'s, then the entire `flush()` API will be performed in a single BEGIN TRANSACTION / COMMIT TRANSACTION block for performance. It is assumed that the `DatabaseStage`('s) will potentially be doing lots of DB work, and thus should be in one transaction.

## Snoopers

SimDB provides the ability to "snoop" a running pipeline to look for data packets that are sitting in between concurrently-running pipeline stages. If your pipeline requires acceptable retrieval time for packets that are in-flight to the database, consider using snoopers to reduce the number of times your code will have to flush the pipeline and access the disk to implement the API.

Pipeline snoopers require a key/UUID data type as well as a return type. The general idea is that while each stage may internally represent data differently, they all must be able to return the same snooped data type that matches the given key/UUID.

Recall the pipeline above which performs the following:
- Stage 1: take `std::vector<double>` and compress into `std::vector<char>`
- Stage 2: write compressed `std::vector<char>` to the database

We will write `snoop()` methods for these two stages that can take any `std::string` UUID and return a copy of the original `std::vector<double>` of associated stats values. Note that if the UUID is found in Stage 1, we can perform a direct copy (faster). If found in Stage 2, we have to decompress the `char` buffer back into the `std::vector<double>` first (slower).

Add this code to the `createPipeline()` method:
```
void StatsCollector::createPipeline(simdb::pipeline::PipelineManager* pipeline_mgr)
{
    auto pipeline = pipeline_mgr->createPipeline("stats-collector-pipeline", this);

    auto compressor = pipeline->addStage<ZlibStage>("compressor");
    auto db_writer = pipeline->addStage<DatabaseStage>("db_writer");
    pipeline->noMoreStages();
    ...

    // using uuid_t = std::string;
    // using values_t = std::vector<double>;
    // using snooper_t = simdb::pipeline::PipelineSnooper<uuid_t, values_t>;
    // std::unique_ptr<snooper_t> pipeline_snooper_;
    pipeline_snooper_ = pipeline->createSnooper<uuid_t, values_t>();
    pipeline_snooper_->addStage(compressor);
    pipeline_snooper_->addStage(db_writer);
}
```

Now write the `snoop()` methods for the two stages. The signature has to be:

`bool snoop(const uuid_t& uuid, values_t& values);`

```
class ZlibStage : public simdb::pipeline::Stage
{
public:
    ...

    // Return true if found (values assigned)
    bool snoop(const uuid_t& uuid, values_t& values)
    {
        // Apply a callback to every incoming uncompressed std::vector<double> into this stage:
        return input_queue_->snoop([&](const StatsValues& uncompressed)
        {
            if (uncompressed.uuid == uuid)
            {
                values = uncompressed.values;

                // Return true to let the snooper know we can stop looking
                return true;
            }

            // Keep looking in this input queue
            return false;
        });
    }
};

class DatabaseStage : public simdb::pipeline::DatabaseStage<StatsCollector>
{
public:
    ...

    // Return true if found (values assigned)
    bool snoop(const uuid_t& uuid, values_t& values)
    {
        // Apply a callback to every compressed std::vector<char> into this stage:
        return input_queue_->snoop([&](const ZlibStatsValues& compressed)
        {
            if (compressed.uuid == uuid)
            {
                // Undo zlib compression
                simdb::decompressData(compressed.bytes, values);

                // Return true to let the snooper know we can stop looking
                return true;
            }

            // Keep looking in this input queue
            return false;
        });
    }
};
```

Now we can add a method to the StatsCollector app which can retrieve any stats values, whether found in-flight to Stage 1, in-flight to Stage 2, or in the database (not successfully snooped):

```
bool StatsCollector::snoopPipeline(const uuid_t& uuid, values_t& values)
{
    if (pipeline_snooper_->snoopAllStages(uuid, values))
    {
        // Assigned 'values' successfully.
        return true;
    }

    // Not found in any stage - check the database (slowest). It is important to
    // note that we may have missed the UUID if it was currently inside any of the
    // stages being processed, i.e. NOT in any ConcurrentQueue's which sit between
    // stages. In other words, it can still be in the pipeline but was not snooped
    // successfully.
    //
    // There are two solutions here:
    //   1) We can use the pipeline_flusher_ flush() method, then query the database.
    //      This will result in a slower snoopPipeline() implementation, but a faster
    //      pipeline.
    //
    //   2) We could implement the stages' run_() methods to store a copy of the data
    //      that is currently being processed, then the snoop() methods could look at
    //      the data copy's UUID and compare it to the requested UUID, and immediately
    //      return it if it matches. If the UUID's don't match, then snoop the stage's
    //      queues. This will result in a faster snoopPipeline() implementation, but a
    //      slower pipeline.
    //
    //      IMPORTANT: You should add a mutex to any stages that use this option to
    //                 protect the copied data member variable.
    //
    //   * It is recommended that the first option is used, since the pipeline speed
    //     is typically more important than the snooping speed. However, if the data
    //     types are small, it may not affect the pipeline speed too much, and you
    //     could use the second option and get the best of both worlds.
    //
    // Use option 1 (flusher):
    pipeline_flusher_->flush();

    // Everything is in the database now. Run a query for the requested UUID:
    auto query = db_mgr_->createQuery("CompressedStats");
    
    std::vector<char> bytes;
    query->select("StatsBlob", bytes);

    query->addConstraintForString("UUID", simdb::Constraints::EQUAL, uuid);
    auto results = query->getResultSet();

    if (results.getNextRecord())
    {
        // Found the record. Undo zlib.
        simdb::decompressData(bytes, values);

        // Assigned 'values' successfully.
        return true;
    }

    // Not found anywhere.
    return false;
}
```

For a complete example, see `examples/PipelineSnoopers/main.cpp`

## Disabling Pipelines

Due to the nature of concurrent systems, there may be times when you need to temporarily disable the pipeline to achieve determinism or better performance. One example of this is in the call to `PipelineSnooper::snoopAllStages()` which disables the pipeline by default:

```
template <typename KeyType, typename SnoopedType>
class PipelineSnooper
{
private:
    using Callback = std::function<bool(const KeyType&, SnoopedType&)>;
    std::vector<Callback> callbacks_;

public:
    bool snoopAllStages(const KeyType& key, SnoopedType& snooped_obj, bool disable_pipeline = true)
    {
        // By default, we take the small performance hit to disable the pipeline
        // stages and threads.
        std::unique_ptr<ScopedRunnableDisabler> disabler = disable_pipeline ?
            pipeline_mgr_->scopedDisableAll() : nullptr;

        // If disable_pipeline=true, everything is disabled until the above object
        // goes out of scope. Reasons to do this include:
        //
        //   1) Determinism: Make it easier to debug pipeline issues. It is difficult
        //      to debug certain issues (like snooping for pipeline packets) while all
        //      the threads are concurrently processing data.
        //
        //   2) Performance: As is the case with stage queue snoopers, it will at times
        //      be more performant to disable everything temporarily. Snoopers naturally
        //      want to find the data of interest as soon as possible, because oftentimes
        //      finding the data in a later stage means you have to "undo" more transforms
        //      to return the original data. We don't want the stages to all be active all
        //      the time, since that would mean that some data could leave Stage 1 while
        //      we are snooping it (unsuccessful snoop), and could leave Stage 2 while we
        //      are snooping it (another unsuccessful snoop), thus we end up "chasing" the
        //      data of interest all the way down the pipeline. With the pipeline disabled
        //      first, we will find the data in the earliest stage possible.

        for (auto& cb : callbacks_)
        {
            if (cb(key, snooped_obj))
            {
                return true;
            }
        }
        return false;
    }

...
};
```

## Async Database Access

TBD

## Thread Sharing

TBD
