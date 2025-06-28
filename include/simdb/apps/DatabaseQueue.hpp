#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/ConcurrentQueue.hpp"

#include <atomic>
#include <functional>
#include <map>
#include <thread>

namespace simdb
{

template <typename DataT, bool OrderByTags>
class DatabaseQueue;

template <typename DataT>
class DatabaseQueue<DataT, false>
{
public:
    using DBFunc = std::function<void(simdb::DatabaseManager&, DataT&&)>;

    DatabaseQueue(simdb::DatabaseManager& db_mgr, DBFunc db_func)
        : db_mgr_(db_mgr)
        , db_func_(db_func)
    {}

    DatabaseQueue() = delete;
    DatabaseQueue(const DatabaseQueue&) = delete;
    DatabaseQueue(DatabaseQueue&&) = delete;
    DatabaseQueue& operator=(const DatabaseQueue&) = delete;
    DatabaseQueue& operator=(DatabaseQueue&&) = delete;
    ~DatabaseQueue() = default;

    void enqueue(DataT&& data)
    {
        data_queue_.emplace(std::move(data));
    }

    void stop()
    {
        is_running_ = false;
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

private:
    void run_()
    {
        while (is_running_)
        {
            flush_();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        flush_();
    }

    void flush_()
    {
        db_mgr_.safeTransaction(
            [&]()
            {
                DataT data;
                while (data_queue_.try_pop(data))
                {
                    db_func_(db_mgr_, std::move(data));
                }
            });
    }

    simdb::DatabaseManager& db_mgr_;
    DBFunc db_func_;
    std::atomic<bool> is_running_ = true;
    simdb::ConcurrentQueue<DataT> data_queue_;
    std::thread thread_{&DatabaseQueue::run_, this};
};

template <typename DataT>
class DatabaseQueue<DataT, true>
{
public:
    using DBFunc = std::function<void(simdb::DatabaseManager&, DataT&&)>;

    DatabaseQueue(simdb::DatabaseManager& db_mgr, DBFunc db_func, uint64_t expected_tag = 0)
        : db_mgr_(db_mgr)
        , db_func_(db_func)
        , expected_tag_(expected_tag)
    {}

    DatabaseQueue() = delete;
    DatabaseQueue(const DatabaseQueue&) = delete;
    DatabaseQueue(DatabaseQueue&&) = delete;
    DatabaseQueue& operator=(const DatabaseQueue&) = delete;
    DatabaseQueue& operator=(DatabaseQueue&&) = delete;
    ~DatabaseQueue() = default;

    void enqueue(uint64_t tag, DataT&& data)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (tag < expected_tag_)
        {
            throw simdb::DBException("Invalid tag");
        }
        ordered_data_map_[tag] = std::move(data);
    }

    void stop()
    {
        is_running_ = false;
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

private:
    void run_()
    {
        while (is_running_)
        {
            flush_();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        flush_();
    }

    void flush_()
    {
        std::vector<DataT> datas;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = ordered_data_map_.find(expected_tag_);
            while (it != ordered_data_map_.end())
            {
                datas.emplace_back(std::move(it->second));
                ordered_data_map_.erase(it);
                it = ordered_data_map_.find(++expected_tag_);
            }
        }

        if (datas.empty())
        {
            return;
        }

        db_mgr_.safeTransaction(
            [&]()
            {
                for (auto& data : datas)
                {
                    db_func_(db_mgr_, std::move(data));
                }
            });
    }

    simdb::DatabaseManager& db_mgr_;
    DBFunc db_func_;
    uint64_t expected_tag_ = 0;
    std::atomic<bool> is_running_ = true;
    std::mutex mutex_;
    std::map<uint64_t, DataT> ordered_data_map_;
    std::thread thread_{&DatabaseQueue::run_, this};
};

} // namespace simdb
