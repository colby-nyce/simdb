// <TreeNode.hpp> -*- C++ -*-

#pragma once

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace simdb {

/// This class is used to build a tree structure from a list of string locations.
/// For example, given the three locations:
///
///     "top.mid1.bottom1"
///     "top.mid1.bottom2"
///     "top.bottom3"
///
/// The buildTree() method will create a TreeNode structure that looks like:
///
///     root
///     |
///     +-- top
///         |
///         +-- mid1
///         |   |
///         |   +-- bottom1
///         |   |
///         |   +-- bottom2
///         |
///         +-- bottom3
struct TreeNode {
    std::string name;
    std::vector<std::unique_ptr<TreeNode>> children;
    const TreeNode *parent = nullptr;

    int db_id = 0;
    int clk_id = 0;
    bool is_collectable = false;

    TreeNode(const std::string &name, const TreeNode *parent = nullptr)
        : name(name), parent(parent) {}

    std::string getLocation() const {
        std::vector<std::string> node_names;
        auto node = this;
        while (node && node->parent) {
            node_names.push_back(node->name);
            node = node->parent;
        }

        std::reverse(node_names.begin(), node_names.end());
        std::ostringstream oss;
        for (size_t idx = 0; idx < node_names.size(); ++idx) {
            oss << node_names[idx];
            if (idx != node_names.size() - 1) {
                oss << ".";
            }
        }

        return oss.str();
    }
};

} // namespace simdb
