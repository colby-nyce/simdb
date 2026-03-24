// <Tree.hpp> -*- C++ -*-

#pragma once

#include "simdb/Exceptions.hpp"

#include <boost/algorithm/string.hpp>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace simdb {

/// \class Tree
/// \brief Generic tree utility supporting typed node creation by dot-delimited path.
class Tree
{
public:
    /// \class TreeNode
    /// \brief Base node type used by Tree and by user-defined node subclasses.
    class TreeNode
    {
    public:
        /// \brief Virtual destructor for polymorphic node hierarchies.
        virtual ~TreeNode() = default;

        /// \brief Construct a node with no parent (typically root).
        /// \param name Node name.
        explicit TreeNode(std::string name) :
            name_(std::move(name))
        {
        }

        /// \brief Construct a node with a parent.
        /// \param name Node name.
        /// \param parent Parent node pointer.
        TreeNode(std::string name, TreeNode* parent) :
            name_(std::move(name)),
            parent_(parent)
        {
        }

        /// \brief Get this node's local name.
        /// \return Node name.
        const std::string& getName() const { return name_; }

        /// \brief Find a child by name.
        /// \param child_name Child node name.
        /// \param must_exist If true, throw when child is not found.
        /// \return Child pointer, or nullptr if not found.
        /// \throw DBException If \a must_exist is true and child is not found.
        TreeNode* getChild(const std::string& child_name, bool must_exist = true)
        {
            for (auto& child : children_)
            {
                if (child->name_ == child_name)
                {
                    return child.get();
                }
            }
            if (must_exist)
            {
                throw DBException("Child node does not exist: ") << child_name
                    << " under parent path '" << getPath() << "'";
            }
            return nullptr;
        }

        /// \brief Find a child by name (const overload).
        /// \param child_name Child node name.
        /// \param must_exist If true, throw when child is not found.
        /// \return Child pointer, or nullptr if not found.
        /// \throw DBException If \a must_exist is true and child is not found.
        const TreeNode* getChild(const std::string& child_name, bool must_exist = true) const
        {
            for (const auto& child : children_)
            {
                if (child->name_ == child_name)
                {
                    return child.get();
                }
            }
            if (must_exist)
            {
                throw DBException("Child node does not exist: ") << child_name
                    << " under parent path '" << getPath() << "'";
            }
            return nullptr;
        }

        /// \brief Find a child by name and cast to a specific node type.
        /// \tparam NodeT Desired child type derived from TreeNode.
        /// \param child_name Child node name.
        /// \param must_exist If true, throw when child is missing or has incompatible type.
        /// \return Typed child pointer, or nullptr if the child does not exist or has a different type.
        /// \throw DBException If \a must_exist is true and the child is missing or has incompatible type.
        template <typename NodeT>
        NodeT* getChildAs(const std::string& child_name, bool must_exist = true)
        {
            static_assert(std::is_base_of_v<TreeNode, NodeT>, "NodeT must derive from Tree::TreeNode");
            auto* child = getChild(child_name, must_exist);
            if (!child && must_exist)
            {
                throw DBException("Child node '") << child_name << "' does not exist under path '"
                    << getPath() << "'";
            }

            auto* typed_child = dynamic_cast<NodeT*>(child);
            if (!typed_child && must_exist)
            {
                throw DBException("Child node exists but has incompatible type: ") << child_name
                    << " under parent path '" << getPath() << "'";
            }
            return typed_child;
        }

        /// \brief Find a child by name and cast to a specific node type (const overload).
        /// \tparam NodeT Desired child type derived from TreeNode.
        /// \param child_name Child node name.
        /// \param must_exist If true, throw when child is missing or has incompatible type.
        /// \return Typed child pointer, or nullptr if the child does not exist or has a different type.
        /// \throw DBException If \a must_exist is true and the child is missing or has incompatible type.
        template <typename NodeT>
        const NodeT* getChildAs(const std::string& child_name, bool must_exist = true) const
        {
            static_assert(std::is_base_of_v<TreeNode, NodeT>, "NodeT must derive from Tree::TreeNode");
            auto* child = getChild(child_name, must_exist);
            if (!child && must_exist)
            {
                throw DBException("Child node '") << child_name << "' does not exist under path '"
                    << getPath() << "'";
            }

            auto* typed_child = dynamic_cast<const NodeT*>(child);
            if (!typed_child && must_exist)
            {
                throw DBException("Child node exists but has incompatible type: ") << child_name
                    << " under parent path '" << getPath() << "'";
            }
            return typed_child;
        }

        /// \brief Get this node's children.
        /// \return Mutable vector of owned child nodes.
        std::vector<std::unique_ptr<TreeNode>>& getChildren() { return children_; }

        /// \brief Get this node's children (const overload).
        /// \return Const vector of owned child nodes.
        const std::vector<std::unique_ptr<TreeNode>>& getChildren() const { return children_; }

        /// \brief Get this node's parent.
        /// \return Parent node pointer, or nullptr for root.
        TreeNode* getParent() { return parent_; }

        /// \brief Get this node's parent (const overload).
        /// \return Parent node pointer, or nullptr for root.
        const TreeNode* getParent() const { return parent_; }

        /// \brief Build this node's dot-delimited path from root.
        /// \details Root node path is an empty string. Non-root paths exclude the root name.
        /// \return Dot-delimited path.
        const std::string& getPath() const
        {
            if (!cached_path_.empty())
            {
                return cached_path_;
            }

            std::vector<std::string> node_names;
            auto node = this;
            while (node != nullptr && !node->name_.empty())
            {
                node_names.push_back(node->name_);
                node = node->parent_;
            }

            std::string path;
            for (auto it = node_names.rbegin(); it != node_names.rend(); ++it)
            {
                if (!path.empty())
                {
                    path += ".";
                }
                path += *it;
            }

            cached_path_ = std::move(path);
            return cached_path_;
        }

    private:
        TreeNode() = default;

        template <typename NodeT, typename... Args>
        NodeT* createChild_(Args&&... args)
        {
            static_assert(std::is_base_of_v<TreeNode, NodeT>, "NodeT must derive from Tree::TreeNode");
            auto child = std::make_unique<NodeT>(std::forward<Args>(args)...);
            auto* raw_ptr = child.get();
            children_.emplace_back(std::move(child));
            return raw_ptr;
        }

        std::string name_;
        std::vector<std::unique_ptr<TreeNode>> children_;
        TreeNode* parent_ = nullptr;
        mutable std::string cached_path_;

        friend class Tree;
    };

    /// \brief Get the tree root node.
    /// \return Mutable root node pointer.
    TreeNode* getRoot() { return &root_; }

    /// \brief Get the tree root node (const overload).
    /// \return Const root node pointer.
    const TreeNode* getRoot() const { return &root_; }

    /// \brief Get or create a leaf node for a dot-delimited path.
    /// \tparam LeafT Node type for the final path segment.
    /// \tparam IntermediateT Node type for all non-leaf segments.
    /// \param path Dot-delimited path, e.g. "top.mid.leaf".
    /// \return Pointer to the leaf node at \a path.
    /// \throw DBException If path is empty, contains empty segments, or if an existing node
    /// has an incompatible runtime type for LeafT/IntermediateT.
    template <typename LeafT, typename IntermediateT = TreeNode>
    LeafT* createNode(const std::string& path)
    {
        static_assert(std::is_base_of_v<TreeNode, LeafT>, "LeafT must derive from Tree::TreeNode");
        static_assert(std::is_base_of_v<TreeNode, IntermediateT>, "IntermediateT must derive from Tree::TreeNode");

        std::vector<std::string> path_parts;
        boost::split(path_parts, path, boost::is_any_of("."));
        if (path_parts.empty())
        {
            throw DBException("Cannot create tree node from an empty path");
        }
        for (const auto& token : path_parts)
        {
            if (token.empty())
            {
                throw DBException("Tree path contains an empty segment: ") << path;
            }
        }

        TreeNode* current = &root_;
        for (size_t idx = 0; idx < path_parts.size(); ++idx)
        {
            const auto& token = path_parts[idx];
            const bool is_leaf = (idx == path_parts.size() - 1);

            auto* existing_child = current->getChild(token, false);
            if (existing_child)
            {
                if (is_leaf)
                {
                    auto* typed_leaf = dynamic_cast<LeafT*>(existing_child);
                    if (!typed_leaf)
                    {
                        throw DBException("Tree node already exists at path but has incompatible type: ")
                            << path;
                    }
                    current = typed_leaf;
                }
                else
                {
                    auto* typed_intermediate = dynamic_cast<IntermediateT*>(existing_child);
                    if (!typed_intermediate)
                    {
                        throw DBException("Intermediate tree node already exists at path but has incompatible type: ")
                            << path;
                    }
                    current = typed_intermediate;
                }
                continue;
            }

            current = is_leaf
                ? static_cast<TreeNode*>(current->createChild_<LeafT>(token, current))
                : static_cast<TreeNode*>(current->createChild_<IntermediateT>(token, current));
        }

        return dynamic_cast<LeafT*>(current);
    }

    /// \brief Get a node at a dot-delimited path and return it by reference.
    /// \tparam NodeT Expected runtime node type at \a path.
    /// \param path Dot-delimited path, e.g. "top.mid.leaf".
    /// \return Reference to the typed node.
    /// \throw DBException If path is invalid, missing, or has incompatible runtime type.
    template <typename NodeT>
    NodeT& getNode(const std::string& path)
    {
        auto* node = tryGetNodeAs<NodeT>(path, true /*must_exist*/);
        return *node;
    }

    /// \brief Get a node at a dot-delimited path and return it by reference (const overload).
    /// \tparam NodeT Expected runtime node type at \a path.
    /// \param path Dot-delimited path, e.g. "top.mid.leaf".
    /// \return Const reference to the typed node.
    /// \throw DBException If path is invalid, missing, or has incompatible runtime type.
    template <typename NodeT>
    const NodeT& getNode(const std::string& path) const
    {
        auto* node = tryGetNodeAs<NodeT>(path, true /*must_exist*/);
        return *node;
    }

    /// \brief Try to get a typed node at a dot-delimited path.
    /// \tparam NodeT Expected runtime node type at \a path.
    /// \param path Dot-delimited path, e.g. "top.mid.leaf".
    /// \param must_exist If true, throw when missing or type-incompatible.
    /// \return Typed node pointer, or nullptr when not found/incompatible and \a must_exist is false.
    /// \throw DBException If path is invalid, or if \a must_exist is true and node is missing/incompatible.
    template <typename NodeT>
    NodeT* tryGetNodeAs(const std::string& path, bool must_exist = true)
    {
        static_assert(std::is_base_of_v<TreeNode, NodeT>, "NodeT must derive from Tree::TreeNode");

        std::vector<std::string> path_parts;
        boost::split(path_parts, path, boost::is_any_of("."));
        if (path_parts.empty())
        {
            throw DBException("Cannot get tree node from an empty path");
        }
        for (const auto& token : path_parts)
        {
            if (token.empty())
            {
                throw DBException("Tree path contains an empty segment: ") << path;
            }
        }

        TreeNode* current = &root_;
        for (const auto& token : path_parts)
        {
            current = current->getChild(token, false);
            if (!current)
            {
                if (must_exist)
                {
                    throw DBException("Tree node does not exist at path: ") << path;
                }
                return nullptr;
            }
        }

        auto* typed_node = dynamic_cast<NodeT*>(current);
        if (!typed_node && must_exist)
        {
            throw DBException("Tree node exists at path but has incompatible type: ") << path;
        }
        return typed_node;
    }

    /// \brief Try to get a typed node at a dot-delimited path (const overload).
    /// \tparam NodeT Expected runtime node type at \a path.
    /// \param path Dot-delimited path, e.g. "top.mid.leaf".
    /// \param must_exist If true, throw when missing or type-incompatible.
    /// \return Typed node pointer, or nullptr when not found/incompatible and \a must_exist is false.
    /// \throw DBException If path is invalid, or if \a must_exist is true and node is missing/incompatible.
    template <typename NodeT>
    const NodeT* tryGetNodeAs(const std::string& path, bool must_exist = true) const
    {
        static_assert(std::is_base_of_v<TreeNode, NodeT>, "NodeT must derive from Tree::TreeNode");

        std::vector<std::string> path_parts;
        boost::split(path_parts, path, boost::is_any_of("."));
        if (path_parts.empty())
        {
            throw DBException("Cannot get tree node from an empty path");
        }
        for (const auto& token : path_parts)
        {
            if (token.empty())
            {
                throw DBException("Tree path contains an empty segment: ") << path;
            }
        }

        const TreeNode* current = &root_;
        for (const auto& token : path_parts)
        {
            current = current->getChild(token, false);
            if (!current)
            {
                if (must_exist)
                {
                    throw DBException("Tree node does not exist at path: ") << path;
                }
                return nullptr;
            }
        }

        auto* typed_node = dynamic_cast<const NodeT*>(current);
        if (!typed_node && must_exist)
        {
            throw DBException("Tree node exists at path but has incompatible type: ") << path;
        }
        return typed_node;
    }

private:
    TreeNode root_;
};

} // namespace simdb
