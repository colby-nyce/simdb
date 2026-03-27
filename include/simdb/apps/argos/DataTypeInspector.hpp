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
    virtual void visitSimpleVariable(const std::string& var_name, PodTypeKind simple_type)
    {
        (void)var_name;
        (void)simple_type;
    }
    virtual void visitEnumVariable(const std::string& enum_name,
                                   EnumBackingKind backing_kind,
                                   const std::vector<std::pair<std::string, std::string>>& name_value_pairs)
    {
        (void)enum_name;
        (void)backing_kind;
        (void)name_value_pairs;
    }
    virtual void visitStructVariable(const std::string& struct_name,
                                     const std::string& parent_struct_name)
    {
        (void)struct_name;
        (void)parent_struct_name;
    }
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
            visitRecursive_(hier->getRoot(), *visitor);
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
                                DataTypeNodeVisitor& visitor)
    {
        if (node.kind == NodeKind::Pod && node.pod_type)
        {
            const std::string var_name = node.field_name.empty() ? node.type_name : node.field_name;
            visitor.visitSimpleVariable(var_name, *node.pod_type);
        }
        else if (node.kind == NodeKind::Enum && node.enum_meta)
        {
            const std::string enum_name = node.field_name.empty() ? node.type_name : node.field_name;
            std::vector<std::pair<std::string, std::string>> name_value_pairs;
            name_value_pairs.reserve(node.enum_meta->members.size());
            for (const auto& member : node.enum_meta->members)
            {
                name_value_pairs.emplace_back(member.name, std::to_string(member.value));
            }
            visitor.visitEnumVariable(enum_name,
                                      node.enum_meta->backing_kind,
                                      name_value_pairs);
        }
        else if (node.kind == NodeKind::Struct)
        {
            const std::string struct_name = node.field_name.empty() ? node.type_name : node.field_name;
            const std::string parent_struct_name =
                (node.parent && node.parent->kind == NodeKind::Struct) ? node.parent->type_name : "";
            visitor.visitStructVariable(struct_name, parent_struct_name);
        }

        for (const auto& child : node.children)
        {
            visitRecursive_(*child, visitor);
        }
    }

    static void injectTinyStringsIntoFields_(const DataTypeNode& node,
                                             TinyStrings<false>* tiny_strings)
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
