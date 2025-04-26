#pragma once

#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

struct DatabaseEntry
{
    std::vector<char> bytes;
    bool compressed = false;
    uint64_t tick = 0;
    std::string notes;
};

class DatabaseManager;

class DatabaseThread : public Thread
{
public:
    DatabaseThread(DatabaseManager* db_mgr)
        : Thread(500)
        , db_mgr_(db_mgr)
    {
    }

    void push(DatabaseEntry&& entry)
    {
        queue_.emplace(std::move(entry));
        startThreadLoop();
    }

    void teardown()
    {
        flush();
        stopThreadLoop();
    }

    uint64_t getNumPending() const
    {
        return queue_.size();
    }

    uint64_t getNumProcessed() const
    {
        return num_processed_;
    }

    void flush();

private:
    void onInterval_() override
    {
        flush();
    }

    ConcurrentQueue<DatabaseEntry> queue_;
    DatabaseManager* db_mgr_;
    uint64_t num_processed_ = 0;
};

} // namespace simdb
