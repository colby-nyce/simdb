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
class ZlibStage : public simdb::pipeline::Stage
{
private:
    // This stage needs an input of uncompressed values:
    simdb::ConcurrentQueue<std::vector<double>>* input_queue_ = nullptr;

    // And after compression, it will need an output for the compressed values:
    simdb::ConcurrentQueue<std::vector<char>>* output_queue_ = nullptr;

public:
    ZlibStage()
    {
        // The addInPort_/addOutPort_() methods use pointer references so that SimDB
        // can assign our I/O ports' simdb::ConcurrentQueue's.
        addInPort_<std::vector<double>>("uncompressed_input", input_queue_);
        addOutPort_<std::vector<char>>("compressed_output", output_queue_);
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
        // when simulation is over (just prior to tearing down pipeline threads).
        //
        // This zlib compressor has no internal buffer/storage, so we don't need
        // to consider this flag for this example stage.

        // If we can read a vector<double> input, compress it and send to the next
        // stage as a vector<char>.
        std::vector<double> uncompressed;
        if (input_queue_->try_pop(uncompressed))
        {
            // #include "simdb/utils/Compress.hpp"
            std::vector<char> compressed;
            simdb::compressData(uncompressed, compressed);

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
        simdb::ConcurrentQueue<std::vector<char>>* input_queue_ = nullptr;

        // For brevity, there is no output queue for this stage. But you can have
        // output queue(s) from database stages if you need them. An example pipeline
        // with DB stage outputs might be something like this:
        //
        //   Sim -> Cache -> Process -> Database ----|
        //            ^                              |
        //            |------------ evict -----------|
        //
        // Where the cache holds onto copies of the original data for fast access,
        // and then evicts from the cache when the data has been written to the
        // database. This probably implies that the data packets are given some
        // sort of UUID, but the design is up to you.

    public:
        DatabaseStage()
        {
            addInPort_<std::vector<char>>("compressed_input", input_queue_);
        }

    private:
        simdb::pipeline::PipelineAction run_(bool force) override
        {
            // Read a compressed char buffer from the input queue
            std::vector<char> compressed;
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
                //       SQL_COLUMNS("StatsBlob"),
                //       SQL_VALUES(compressed));
                //
                // There is little reason to use DatabaseManager, so we
                // will typically use prepared statements:
                //
                auto inserter = getTableInserter_("CompressedStats");
                inserter->setColumnValue(0, compressed);
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
    simdb::ConcurrentQueue<std::vector<double>>* pipeline_head_ = nullptr;

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
        // anything. Store the pipeline input queue now.
        pipeline_head_ = pipeline->getInPortQueue<std::vector<double>>("compressor.uncompressed_input");

        // Note: There is no reason you cannot use multiple input ports for the pipeline's
        // first stage. For instance, your first stage might operate on tuples of values
        // that come into the pipeline at different times, thus the stage would have more
        // than one input queue, and will forward along / process the whole tuple when it
        // becomes available. In other words, such a pipeline would start out with a mux.
        // See examples/MultiPortStages/main.cpp
    }

private:
    class ZlibStage ...
    class DatabaseStage ...
};
```

# Pipeline Extras

## Flushers

TBD

## Snoopers

TBD

## Async Database Access

TBD

## Thread Sharing

TBD
