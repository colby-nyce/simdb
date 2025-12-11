// <Pipeline.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Stage.hpp"
#include "simdb/sqlite/DatabaseManager.hpp"

namespace simdb::pipeline {

class Flusher
{
public:
    Flusher(const std::vector<Stage*>& stages)
        : stages_(stages)
    {
        if (stages_.empty())
        {
            throw DBException("No stages given to Flusher");
        }
    }

    virtual ~Flusher() = default;

    virtual RunnableOutcome flush()
    {
        RunnableOutcome outcome = RunnableOutcome::SLEEP;

        bool continue_while = true;
        do
        {
            continue_while = false;
            for (auto stage : stages_)
            {
                constexpr auto force = true;
                if (stage->processAll(force) == RunnableOutcome::PROCEED)
                {
                    outcome = RunnableOutcome::PROCEED;
                    continue_while = true;
                }
            }
        } while (continue_while);

        return outcome;
    }

private:
    std::vector<Stage*> stages_;
};

class FlusherWithTransaction : public Flusher
{
public:
    FlusherWithTransaction(const std::vector<Stage*>& stages, DatabaseManager* db_mgr)
        : Flusher(stages)
        , db_mgr_(db_mgr)
    {
        bool has_db_stage = false;
        for (auto stage : stages)
        {
            if (dynamic_cast<DatabaseStageBase*>(stage))
            {
                has_db_stage = true;
            }
        }

        if (!has_db_stage)
        {
            throw DBException("Cannot use FlusherWithTransaction - there are no database stages!");
        }
    }

    RunnableOutcome flush() override
    {
        auto outcome = RunnableOutcome::SLEEP;
        db_mgr_->safeTransaction([&]()
        {
            outcome = Flusher::flush();
        });
        return outcome;
    }

private:
    DatabaseManager* db_mgr_ = nullptr;
};

} // namespace simdb::pipeline
