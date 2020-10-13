#ifndef BPT_H_
#define BPT_H_

#include "page.h"

#include <optional>
#include <string>
#include <tuple>
#include <vector>

class BPTree
{
 public:
    static constexpr int INTERNAL_ORDER = PAGE_BRANCHES_IN_PAGE + 1;
    static constexpr int LEAF_ORDER = PAGE_DATA_IN_PAGE + 1;

    static constexpr int MERGE_THRESHOLD = 0;

 public:
    static BPTree& get();

    [[nodiscard]] bool open(const std::string& filename);
    [[nodiscard]] bool is_open() const;

    [[nodiscard]] bool insert(const page_data_t& record);
    [[nodiscard]] bool remove(int64_t key);
    [[nodiscard]] std::optional<page_data_t> find(int64_t key) const;
    [[nodiscard]] std::vector<page_data_t> find_range(int64_t key_start,
                                                      int64_t key_end) const;

    [[nodiscard]] std::string to_string() const;

 private:
    [[nodiscard]] Page make_node(bool is_leaf) const;

    [[nodiscard]] Page find_leaf(int64_t key) const;
    [[nodiscard]] int path_to_root(pagenum_t child) const;

    // insert operation helper methods
    [[nodiscard]] bool insert_into_leaf(Page& leaf, const page_data_t& record);
    [[nodiscard]] bool insert_into_parent(Page& left, Page& right, int64_t key);
    [[nodiscard]] bool insert_into_new_root(Page& left, Page& right,
                                            int64_t key);
    [[nodiscard]] bool insert_into_node(Page& parent, int left_index,
                                        Page& right, int64_t key);
    [[nodiscard]] bool insert_into_leaf_after_splitting(
        Page& leaf, const page_data_t& record);
    [[nodiscard]] bool insert_into_node_after_splitting(Page& old,
                                                        int left_index,
                                                        Page& right,
                                                        int64_t key);

    // delete operation helper methods
    [[nodiscard]] bool delete_entry(Page node, int64_t key);
    void remove_branch_from_internal(Page& node, int64_t key);
    void remove_record_from_leaf(Page& node, int64_t key);
    [[nodiscard]] bool adjust_root(Page& header, Page& root);
    [[nodiscard]] bool coalesce_nodes(Page& parent, Page& left, Page& right,
                                      int64_t k_prime);
    [[nodiscard]] bool redistribute_nodes(Page& parent, Page& left,
                                          Page& right, int k_prime_index,
                                          int64_t k_prime);

 private:
    pagenum_t root_page_{ NULL_PAGE_NUM };
};

#endif  // BPT_H_
