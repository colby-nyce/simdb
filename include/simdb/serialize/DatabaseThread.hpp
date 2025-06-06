#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
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

template <typename PipelineDataT>
using EndOfPipelineCallback = std::function<void(DatabaseManager*, PipelineDataT&&)>;

#define END_OF_PIPELINE_CALLBACK(ApplicationType, method_name) \
    std::bind(&ApplicationType::method_name, dynamic_cast<ApplicationType*>(__this__), \
              std::placeholders::_1, std::placeholders::_2)

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

/// This class serves as the last stage in a database pipeline.
template <typename PipelineDataT>
class DatabaseThread : public Thread
{
public:
    DatabaseThread(DatabaseManager* db_mgr, EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback)
        : Thread(500)
        , db_mgr_(db_mgr)
        , end_of_pipeline_callback_(end_of_pipeline_callback)
    {
    }

    void push(PipelineDataT&& entry)
    {
        queue_.emplace(std::move(entry));
        startThreadLoop();
    }

    void teardown()
    {
        flush();
        stopThreadLoop();
    }

    void queueWork(const AnyDatabaseWork& work)
    {
        work_queue_.emplace(work);
    }

    void flush()
    {
        db_mgr_->safeTransaction(
            [&]()
            {
                PipelineDataT entry;
                while (queue_.try_pop(entry))
                {
                    end_of_pipeline_callback_(db_mgr_, std::move(entry));
                }

                AnyDatabaseWork work;
                while (work_queue_.try_pop(work))
                {
                    work(db_mgr_);
                }

                return true;
            });
    }

private:
    void onInterval_() override
    {
        flush();
    }

    ConcurrentQueue<PipelineDataT> queue_;
    ConcurrentQueue<AnyDatabaseWork> work_queue_;
    DatabaseManager* db_mgr_;
    EndOfPipelineCallback<PipelineDataT> end_of_pipeline_callback_;
};

} // namespace simdb
