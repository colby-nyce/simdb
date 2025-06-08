#pragma once

#include "simdb/pipeline/DatabaseThread.hpp"
#include "simdb/utils/Compress.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

/// One or more of these threads work on the AsyncPipeline's queue of pending
/// DatabaseEntry objects. Each of these threads can have its own compression
/// level, and compression can also be disabled and re-enabled at runtime.
///
/// Users typically do not need to create these threads directly, as they are
/// created by the AsyncPipeline which is the class to use for creating database
/// pipelines.
class CompressionThread : public Thread
{
public:
    CompressionThread(ConcurrentQueue<DatabaseEntry>& queue, DatabaseThread& db_thread,
                      CompressionLevel compression_level = CompressionLevel::DEFAULT)
        : Thread(500)
        , queue_(queue)
        , db_thread_(db_thread)
    {
    }

    /// Reconfigure the compression level for this thread.
    void setCompressionLevel(CompressionLevel level)
    {
        compression_level_ = level;
    }

    /// Disable compression for this thread.
    void disableCompression()
    {
        setCompressionLevel(CompressionLevel::DISABLED);
    }

    /// Enable compression for this thread.
    void enableCompression(CompressionLevel compression_level = CompressionLevel::DEFAULT)
    {
        setCompressionLevel(compression_level);
    }

private:
    /// Called every 500ms. Flush whatever we can from the queue, compress it,
    /// and send it to the database thread. Remember that this queue is a shared
    /// reference across all CompressionThread objects (and is owned by the AsyncPipeline).
    void onInterval_() override
    {
        DatabaseEntry entry;
        while (queue_.try_pop(entry))
        {
            compress_(entry);
            db_thread_.process(std::move(entry));
        }
    }

    /// Compress the entry if we are able.
    void compress_(DatabaseEntry& entry)
    {
        if (entry.compressed)
        {
            return;
        }

        compressDataVec(entry.data_ptr, entry.num_bytes, compressed_bytes_, compression_level_);
        entry.container = compressed_bytes_;
        entry.data_ptr = compressed_bytes_.data();
        entry.num_bytes = compressed_bytes_.size();
        entry.compressed = true;
    }

    ConcurrentQueue<DatabaseEntry>& queue_;
    DatabaseThread& db_thread_;
    std::vector<char> compressed_bytes_;
    CompressionLevel compression_level_;
};

} // namespace simdb
