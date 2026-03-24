// <ArgosRecord.hpp> -*- C++ -*-

#pragma once

#include "simdb/utils/StreamBuffer.hpp"

#include <cstdint>
#include <vector>

namespace simdb::collection {

/// \class ArgosRecord
/// \brief Raw data held in the SimDB collection "black box". Sent to the
/// database for as long as the Status isn't set to DONT_READ.
class ArgosRecord
{
public:
    /// Create with the collectable ID (unique)
    explicit ArgosRecord(uint16_t c_id) : c_id_(c_id) {}

    /// Get our unique collectable ID
    uint16_t getID() const { return c_id_; }

    /// Start a new buffer for collected data
    StreamBuffer startCollection(bool write_cid = true)
    {
        StreamBuffer buffer(databuf_);
        if (write_cid) {
            buffer << getID();
        }
        return buffer;
    }

    /// Af this record is active, append its data to the output buffer.
    void onSweep(std::vector<char> & swept_data)
    {
        if (status_ != Status::DONT_READ)
        {
            swept_data.insert(swept_data.end(), databuf_.begin(), databuf_.end());
        }

        if (status_ == Status::READ_ONCE)
        {
            status_ = Status::DONT_READ;
        }
    }

    /// Stop adding our data to the output buffer - see onSweep()
    void deactivate()
    {
        status_ = Status::DONT_READ;
    }

private:
    enum class Status { READ, READ_ONCE, DONT_READ };
    Status status_ = Status::DONT_READ;

    /// Unique ID (collectable ID)
    const uint16_t c_id_;

    /// Raw collected data held in a char buffer
    std::vector<char> databuf_;
};

} // namespace simdb::collection
