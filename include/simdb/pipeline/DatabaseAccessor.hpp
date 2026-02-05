// <DatabaseAccessor.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/AsyncDatabaseAccessor.hpp"

namespace simdb::pipeline {

/// This class provides access to the DatabaseManager as well as prepared
/// INSERT objects that are specific to a particular App's schema.
class DatabaseAccessor {
  public:
    DatabaseAccessor(DatabaseManager *db_mgr) : db_mgr_(db_mgr) {}

    DatabaseManager *getDatabaseManager() const { return db_mgr_; }

    template <typename App> PreparedINSERT *getTableInserter(const std::string &tbl_name) {
        auto &inserters = tbl_inserters_by_app_[App::NAME];
        if (inserters.empty()) {
            Schema schema;
            App::defineSchema(schema);

            for (const auto &tbl : schema.getTables()) {
                std::vector<std::string> col_names;
                for (const auto &col : tbl.getColumns()) {
                    col_names.emplace_back(col->getName());
                }

                SqlTable table(tbl.getName());
                SqlColumns columns(col_names);
                auto inserter = db_mgr_->prepareINSERT(std::move(table), std::move(columns));
                inserters[tbl.getName()] = std::move(inserter);
            }
        }

        auto &inserter = inserters[tbl_name];
        if (!inserter) {
            throw DBException("Table does not exist in schema for app: ") << App::NAME;
        }

        return inserter.get();
    }

  private:
    DatabaseManager *db_mgr_ = nullptr;

    using TableInserters = std::unordered_map<std::string, std::unique_ptr<PreparedINSERT>>;
    std::unordered_map<std::string, TableInserters> tbl_inserters_by_app_;
};

} // namespace simdb::pipeline
