#pragma once

#include "simdb/apps/App.hpp"
#include "simdb/apps/AppPipeline.hpp"
#include "simdb/utils/VectorSerializer.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb
{

/// Base class for SimDB applications that use a pipeline for processing.
class PipelineApp : public App
{
public:
    PipelineApp(AppPipeline& pipeline, PipelineChain serialization_chain = PipelineChain())
        : pipeline_(pipeline)
        , serialization_chain_(serialization_chain)
        , serialization_stage_(pipeline.getSerializationStage())
    {
        // Now that we are in the base class, reverse the chain to ensure
        // that we run the chain links in the correct order. The base class
        // should be the first link in the chain and on down the class
        // hierarchy. For instance, a base class might handle writing to
        // the database, and subclasses will need the DB ID for their
        // entry processing functions.
        serialization_chain_.reverse();
    }

    DatabaseManager* getDatabaseManager() const
    {
        return pipeline_.getDatabaseManager();
    }

    void process(uint64_t tick, std::vector<char>&& data, PipelineFunc on_serialized = nullptr)
    {
        PipelineEntry entry(tick, pipeline_.getDatabaseManager(), std::move(data));
        entry.setOwningApp(this);
        auto& chain = entry.getStageChain(serialization_stage_);
        chain += serialization_chain_;
        if (on_serialized)
        {
            chain += on_serialized;
        }
        chain += RetireEntry;
        pipeline_.processEntry(std::move(entry));
    }

    template <typename T>
    void process(uint64_t tick, const std::vector<T>& data, PipelineFunc on_serialized = nullptr)
    {
        VectorSerializer<T> serializer = createVectorSerializer<T>(&data);
        process(tick, std::move(serializer), on_serialized);
    }

    template <typename T>
    void process(uint64_t tick, VectorSerializer<T>&& serializer, PipelineFunc on_serialized = nullptr)
    {
        std::vector<char> data = serializer.release();
        process(tick, std::move(data), on_serialized);
    }

    template <typename T>
    VectorSerializer<T> createVectorSerializer(const std::vector<T>* initial_data = nullptr)
    {
        std::vector<char> serialized_data;
        reusable_buffers_.try_pop(serialized_data);
        return VectorSerializer<T>(std::move(serialized_data), initial_data);
    }

    void teardown() override final
    {
        onPreTeardown_();
        pipeline_.teardown();
        onPostTeardown_();
    }

private:
    virtual void onPreTeardown_() {}
    virtual void onPostTeardown_() {}

    static void RetireEntry(PipelineEntry& entry)
    {
        static_cast<PipelineApp*>(entry.getOwningApp())->retireEntry(entry);
    }

    void retireEntry(PipelineEntry& entry)
    {
        entry.retire(reusable_buffers_);
    }

    AppPipeline& pipeline_;
    PipelineChain serialization_chain_;
    ConcurrentQueue<std::vector<char>> reusable_buffers_;
    PipelineStage* serialization_stage_ = nullptr;
};

} // namespace simdb
