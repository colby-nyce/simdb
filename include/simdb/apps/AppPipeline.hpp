#pragma once

#include "simdb/pipeline/Pipeline.hpp"

namespace simdb
{

/// This class wraps the generic Pipeline class specifically for
/// SimDB applications (PipelineApp and its subclasses). It will
/// preconfigure a two-stage pipeline: one thread for compression
/// and one thread for serialization.
class AppPipeline
{
public:
    /// Construct with a retire chain that will be used
    /// to serialize entries (aka the last stage's chain).
    AppPipeline(DatabaseManager* db_mgr)
        : db_mgr_(db_mgr)
    {
        compression_stage_ = pipeline_.addStage(CompressEntry);
        serialization_stage_ = pipeline_.addStage();
        pipeline_.finalize();
    }

    DatabaseManager* getDatabaseManager() const
    {
        return db_mgr_;
    }

    PipelineStage* getSerializationStage() const
    {
        return serialization_stage_;
    }

    void processEntry(PipelineEntryBase&& entry)
    {
        compression_stage_->getInputQueue()->push(std::move(entry));
    }

    void teardown()
    {
        pipeline_.teardown();
    }

private:
    static void CompressEntry(PipelineEntryBase& entry)
    {
        entry.compress();
    }

    DatabaseManager* db_mgr_ = nullptr;
    Pipeline pipeline_;
    PipelineStage* compression_stage_ = nullptr;
    PipelineStage* serialization_stage_ = nullptr;
};

} // namespace simdb
