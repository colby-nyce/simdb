// <TinyStrings.hpp> -*- C++ -*-

#pragma once

#include "simdb/sqlite/DatabaseManager.hpp"
#include "simdb/utils/DeferredLock.hpp"
#include <mutex>

namespace simdb {

/// To keep SimDB collection as fast and small as possible, we serialize strings
/// not as actual strings, but as ints. This class is used to map strings to ints,
/// and is periodically serialized to a database table.
template <bool MutexProtect = false> class TinyStrings {
  public:
    TinyStrings(DatabaseManager *db_mgr, const std::string &table_name = "TinyStringIDs")
        : db_mgr_(db_mgr), table_name_(table_name) {
        const auto &schema = db_mgr_->getSchema();
        if (!schema.hasTable(table_name_)) {
            Schema append_schema;
            using dt = simdb::SqlDataType;
            auto &tbl = append_schema.addTable(table_name_);
            tbl.addColumn("StringValue", dt::string_t);
            tbl.addColumn("StringID", dt::int32_t);
            db_mgr->appendSchema(append_schema);
        }
    }

    std::pair<uint32_t, bool> insert(const std::string &s) {
        DeferredLock<std::mutex> lock(mutex_);
        if constexpr (MutexProtect) {
            lock.lock();
        }

        auto iter = map_->find(s);
        if (iter == map_->end()) {
            return std::make_pair(getStringID_(s), true);
        } else {
            return std::make_pair(getStringID_(s), false);
        }
    }

    uint32_t getStringID(const std::string &s) {
        DeferredLock<std::mutex> lock(mutex_);
        if constexpr (MutexProtect) {
            lock.lock();
        }

        return getStringID_(s);
    }

    /// Serialize the current string map to the database.
    void serialize() {
        db_mgr_->safeTransaction([&]() {
            DeferredLock<std::mutex> lock(mutex_);
            if constexpr (MutexProtect) {
                lock.lock();
            }

            for (const auto &[string_id, string_val] : unserialized_map_) {
                db_mgr_->INSERT(SQL_TABLE(table_name_), SQL_COLUMNS("StringValue", "StringID"),
                                SQL_VALUES(string_val, string_id));
            }

            unserialized_map_.clear();
        });
    }

  private:
    uint32_t getStringID_(const std::string &s) {
        auto iter = map_->find(s);
        if (iter == map_->end()) {
            uint32_t id = map_->size();
            map_->insert({s, id});
            unserialized_map_.insert({id, s});
            return id;
        } else {
            return iter->second;
        }
    }

    using string_map_t = std::shared_ptr<std::unordered_map<std::string, uint32_t>>;
    using unserialized_string_map_t = std::unordered_map<uint32_t, std::string>;

    string_map_t map_ = std::make_shared<std::unordered_map<std::string, uint32_t>>();
    unserialized_string_map_t unserialized_map_;

    DatabaseManager *db_mgr_;
    std::string table_name_;
    std::mutex mutex_;
};

} // namespace simdb
