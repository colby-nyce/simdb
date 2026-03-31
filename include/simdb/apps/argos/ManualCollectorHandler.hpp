// <ManualCollectorHandler.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/PipelineStager.hpp"
#include <vector>

namespace simdb::collection {

class ManualCollectorHandler
{
public:
    ManualCollectorHandler(size_t heartbeat, std::vector<char>&& bytes)
        : heartbeat_(heartbeat)
        , bytes_last_seen_(std::move(bytes))
        , has_value_(!bytes_last_seen_.empty())
    {}

    // Update the last-seen manual payload for this collectable.
    void setBytes(std::vector<char>&& bytes)
    {
        bytes_last_seen_ = std::move(bytes);
        has_value_ = !bytes_last_seen_.empty();
    }

    // Decide whether to append this collectable's bytes into the auto-collected
    // stream at the current "tick". The time_point is intentionally unused in
    // this logic; cadence is tracked purely by a counter.
    void appendToAutoCollection(const TimePointBase*, std::vector<char>& auto_collected)
    {
        if (!has_value_)
        {
            return;
        }

        // First-ever emit: always write the value immediately.
        if (!has_ever_emitted_)
        {
            auto_collected.insert(auto_collected.end(),
                                  bytes_last_seen_.begin(),
                                  bytes_last_seen_.end());
            bytes_last_emitted_ = bytes_last_seen_;
            has_ever_emitted_ = true;
            ticks_since_emit_ = 0;
            return;
        }

        // If the value changed since the last emit, write it immediately.
        if (bytes_last_seen_ != bytes_last_emitted_)
        {
            auto_collected.insert(auto_collected.end(),
                                  bytes_last_seen_.begin(),
                                  bytes_last_seen_.end());
            bytes_last_emitted_ = bytes_last_seen_;
            ticks_since_emit_ = 0;
            return;
        }

        // Value is unchanged; only emit on heartbeat boundaries.
        ++ticks_since_emit_;
        if (ticks_since_emit_ >= heartbeat_)
        {
            auto_collected.insert(auto_collected.end(),
                                  bytes_last_seen_.begin(),
                                  bytes_last_seen_.end());
            ticks_since_emit_ = 0;
        }
    }

    // Pass 1: enable/disable semantics are not wired into the handler yet.
    // This overload matches the Collection::collectableEnabledAt() caller.
    void collectableEnabledAt(std::shared_ptr<TimePointBase>, bool)
    {
        // no-op for now
    }

private:
    const size_t heartbeat_;

    // Last value seen via setBytes()
    std::vector<char> bytes_last_seen_;
    bool has_value_ = false;

    // Last value we actually emitted into the stream
    std::vector<char> bytes_last_emitted_;
    bool has_ever_emitted_ = false;

    // Number of "ticks" since the last emit, for unchanged values.
    size_t ticks_since_emit_ = 0;
};

} // namespace simdb::collection
