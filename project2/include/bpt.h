#ifndef BPT_H_
#define BPT_H_

#include "file.h"

#include <optional>
#include <tuple>
#include <string>

class BPTree
{
public:
    static constexpr size_t INTERNAL_ORDER = PAGE_BRANCHES_IN_PAGE + 1;
    static constexpr size_t LEAF_ORDER = PAGE_DATA_IN_PAGE + 1;

    using node_t = std::tuple<pagenum_t, page_ptr_t>;

public:
    static BPTree& get();

    void sync_with_file();

    [[nodiscard]] bool insert(const page_data_t& record);
    [[nodiscard]] bool remove(int64_t key);
    [[nodiscard]] std::optional<page_data_t> find(int64_t key);

    std::string to_string() const;

private:
    node_t make_node(bool is_leaf) const;

	node_t find_leaf(int64_t key) const;
    int path_to_root(pagenum_t child) const;

    // insert operation helper methods
	void insert_into_leaf(node_t& leaf_node, const page_data_t& record);
    void insert_into_parent(node_t& left_node, node_t& right_node, int64_t key);
    void insert_into_new_root(node_t& left_node, node_t& right_node, int64_t key);
    void insert_into_node(node_t& parent_node, int left_index, pagenum_t right_num, int64_t key);
    void insert_into_leaf_after_splitting(node_t& leaf_node, const page_data_t& record);
    void insert_into_node_after_splitting(node_t& old_node, int left_index, pagenum_t right_num, int64_t key);

private:
    pagenum_t root_page_{ NULL_PAGE_NUM };
};

#endif  // BPT_H_
