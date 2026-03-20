// <Collections.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/argos/Collection.hpp"
#include "simdb/Exceptions.hpp"

#include <map>
#include <memory>
#include <string>

namespace simdb::collection {

//! \class Collections
//! \brief This class holds separate Collection's for each clock
//! domain across all collectables.
class Collections
{
public:
    //! \brief Add a collection for one clock domain
    //! \param clk_name Clock name
    //! \param clk_period Clock period
    //! \return Returns the newly-added collection, or the existing collection for
    //! this clock if it already had one
    //! \throw Throws if this clock already had a collection, but with a different
    //! clock period
    template <typename TimeT>
    TimestampedCollection<TimeT>* addCollection(const std::string& clk_name, size_t clk_period)
    {
        if (clk_periods_.count(clk_name) && clk_periods_[clk_name] != clk_period)
        {
            throw DBException("Cannot add collection for clock '")
                << clk_name << "' with period " << clk_period << ". This clock "
                << "already has a collection with period " << clk_periods_[clk_name];
        }

        auto& collection = clk_collections_[clk_name];
        if (!collection)
        {
            collection = std::make_unique<TimestampedCollection<TimeT>>();
            clk_periods_[clk_name] = clk_period;
        }
        return static_cast<TimestampedCollection<TimeT>*>(collection.get());
    }

    //! \brief Get a collection previously created by addCollection()
    template <typename TimeT>
    TimestampedCollection<TimeT>* getCollection(const std::string& clk_name) const
    {
        auto it = clk_collections_.find(clk_name);
        if (it == clk_collections_.end())
        {
            return nullptr;
        }
        if (auto collection = dynamic_cast<TimestampedCollection<TimeT>*>(it->second.get()))
        {
            return collection;
        }
        throw DBException("TimestampedCollection exists for clock '") << clk_name
            << "' but the dynamic_cast failed";
    }

private:
    std::map<std::string, std::unique_ptr<Collection>> clk_collections_;
    std::map<std::string, size_t> clk_periods_;
};

} // namespace simdb::collection
