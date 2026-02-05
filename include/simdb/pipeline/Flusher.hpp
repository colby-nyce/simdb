// <Flusher.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Stage.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb::pipeline {

/// This class serves as a utility to flush pipeline stages
/// in a specific order. Create with Pipeline::createFlusher().
class Flusher {
  protected:
    Flusher(const std::vector<Stage *> &stages) : stages_(stages) {
        if (stages_.empty()) {
            throw DBException("No stages given to Flusher");
        }
    }

    friend class Pipeline;

  public:
    virtual ~Flusher() = default;

    virtual PipelineAction flush() {
        PipelineAction outcome = PipelineAction::SLEEP;

        bool continue_while = true;
        do {
            continue_while = false;
            for (auto stage : stages_) {
                constexpr auto force = true;
                if (stage->processAll(force) == PipelineAction::PROCEED) {
                    outcome = PipelineAction::PROCEED;
                    continue_while = true;
                }
            }
        } while (continue_while);

        return outcome;
    }

  private:
    std::vector<Stage *> stages_;
};

/// This subclass is instantiated by Pipeline::createFlusher() when
/// any of the stages that need flushing are database stages. It only
/// serves to put the flush() inside a BEGIN/COMMIT TRANSACTION block
/// for performance (avoid many small transactions).
class FlusherWithTransaction : public Flusher {
  private:
    FlusherWithTransaction(const std::vector<Stage *> &stages, DatabaseManager *db_mgr)
        : Flusher(stages), db_mgr_(db_mgr) {
        bool has_db_stage = false;
        for (auto stage : stages) {
            if (dynamic_cast<DatabaseStageBase *>(stage)) {
                has_db_stage = true;
            }
        }

        if (!has_db_stage) {
            throw DBException("Cannot use FlusherWithTransaction - there are no database stages!");
        }
    }

    friend class Pipeline;

  public:
    PipelineAction flush() override {
        auto outcome = PipelineAction::SLEEP;
        db_mgr_->safeTransaction([&]() { outcome = Flusher::flush(); });
        return outcome;
    }

  private:
    DatabaseManager *db_mgr_ = nullptr;
};

} // namespace simdb::pipeline
