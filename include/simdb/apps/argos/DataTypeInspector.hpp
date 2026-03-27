#pragma once

#include "simdb/apps/argos/DataTypeHierarchy.hpp"
#include "simdb/apps/argos/ArgosCollect.hpp"
#include "simdb/utils/Demangle.hpp"
#include "simdb/utils/TinyStrings.hpp"

#include <map>
#include <memory>
#include <string>
#include <type_traits>

namespace simdb::collection {

class DataTypeNodeVisitor
{
public:
    virtual ~DataTypeNodeVisitor() = default;
    virtual void visit(const DataTypeNode& node, int depth) = 0;
};

class DataTypeInspector
{
public:
    template <typename Type>
    void registerType()
    {
        using value_t = std::remove_cv_t<std::remove_reference_t<Type>>;
        const auto type_name = simdb::demangle_type<value_t>();
        if (root_hierarchies_.count(type_name))
        {
            return;
        }

        auto hier = createDataTypeHier<value_t>();
        if (tiny_strings_)
        {
            hier->setTinyStrings(tiny_strings_.get());
        }
        root_hierarchies_.emplace(type_name, std::move(hier));
    }

    void acceptVisitor(DataTypeNodeVisitor* visitor) const
    {
        if (!visitor)
        {
            return;
        }
        for (const auto& [_, hier] : root_hierarchies_)
        {
            visitRecursive_(hier->getRoot(), *visitor, 0);
        }
    }

    void connect(simdb::DatabaseManager* db_mgr)
    {
        tiny_strings_ = std::make_unique<simdb::TinyStrings<>>(db_mgr);
        for (auto& [_, hier] : root_hierarchies_)
        {
            hier->setTinyStrings(tiny_strings_.get());
            injectTinyStringsIntoFields_(hier->getRoot(), tiny_strings_.get());
        }
    }

    void teardown()
    {
        if (tiny_strings_)
        {
            tiny_strings_->serialize();
        }
    }

private:
    static void visitRecursive_(const DataTypeNode& node,
                                DataTypeNodeVisitor& visitor,
                                int depth)
    {
        visitor.visit(node, depth);
        for (const auto& child : node.children)
        {
            visitRecursive_(*child, visitor, depth + 1);
        }
    }

    static void injectTinyStringsIntoFields_(const DataTypeNode& node,
                                             ::simdb::TinyStrings<false>* tiny_strings)
    {
        if (node.source_field)
        {
            auto* field = static_cast<ArgosFieldBase*>(node.source_field);
            field->setTinyStrings(tiny_strings);
        }
        for (const auto& child : node.children)
        {
            injectTinyStringsIntoFields_(*child, tiny_strings);
        }
    }

    std::map<std::string, std::unique_ptr<DataTypeHierarchyBase>> root_hierarchies_;
    std::unique_ptr<simdb::TinyStrings<>> tiny_strings_;
};

} // namespace simdb::collection
