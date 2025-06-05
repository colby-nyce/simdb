#pragma once

#include <functional>
#include "simdb/utils/ConcurrentQueue.hpp"
#include "simdb/utils/Thread.hpp"

namespace simdb
{

//! Callback function for database entry post-processing. If you pass in a function
//! pointer to the CollectionMgr::sweep() method, your callback will be invoked
//! as soon as the record is written to the database.
//!
//! \param datablob_db_id The database ID of the written record (CollectionRecords table).
//!
//! \param tick The tick at which the record was written to the database.
//!
//! \param user_data Optional user data (e.g. a "this" pointer) that was passed into sweep().
//!
//! \note This callback is always called inside a BEGIN/COMMIT TRANSACTION block on the database thread.
typedef void (*DatabaseEntryCallback)(const int datablob_db_id, const uint64_t tick, void* user_data);

struct DatabaseEntry
{
    std::vector<char> bytes;
    bool compressed = false;
    uint64_t tick = 0;
    DatabaseEntryCallback post_process_callback = nullptr;
    void* post_process_user_data = nullptr;
};

class DatabaseManager;

using AnyDatabaseWork = std::function<void(DatabaseManager*)>;

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

    void queueWork(const AnyDatabaseWork& work)
    {
        work_queue_.emplace(work);
    }

    void flush();

private:
    void onInterval_() override
    {
        flush();
    }

    ConcurrentQueue<DatabaseEntry> queue_;
    ConcurrentQueue<AnyDatabaseWork> work_queue_;
    DatabaseManager* db_mgr_;
    uint64_t num_processed_ = 0;
};

} // namespace simdb
