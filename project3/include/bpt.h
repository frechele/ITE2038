#ifndef BPT_H_
#define BPT_H_

#include "page.h"

#include <optional>
#include <string>
#include <vector>

class BPTree
{
 public:
    static constexpr int INTERNAL_ORDER = PAGE_BRANCHES_IN_PAGE + 1;
    static constexpr int LEAF_ORDER = PAGE_DATA_IN_PAGE + 1;

    static constexpr int MERGE_THRESHOLD = 0;

 public:
    [[nodiscard]] static int open_table(const std::string& filename);
    [[nodiscard]] static bool close_table(table_id_t table_id);
    [[nodiscard]] static bool is_open(table_id_t table_id);

    [[nodiscard]] static bool insert(table_id_t table_id,
                                     const page_data_t& record);
    [[nodiscard]] static bool remove(table_id_t table_id, int64_t key);
    [[nodiscard]] static std::optional<page_data_t> find(table_id_t table_id,
                                                         int64_t key);
    [[nodiscard]] static std::vector<page_data_t> find_range(
        table_id_t table_id, int64_t key_start, int64_t key_end);

    [[nodiscard]] static std::string to_string(table_id_t table_id);

 private:
    [[nodiscard]] static table_page_t make_node(Page& header, bool is_leaf);

    [[nodiscard]] static std::optional<page_data_t> find(Page& header,
                                                         int64_t key);
    [[nodiscard]] static table_page_t find_leaf(Page& header, int64_t key);
    [[nodiscard]] static int path_to_root(Page& header, pagenum_t child);

    // insert operation helper methods
    [[nodiscard]] static bool insert_into_leaf(Page& header, Page& leaf,
                                               const page_data_t& record);
    [[nodiscard]] static bool insert_into_parent(Page& header, Page& left,
                                                 Page& right, int64_t key);
    [[nodiscard]] static bool insert_into_new_root(Page& header, Page& left,
                                                   Page& right, int64_t key);
    [[nodiscard]] static bool insert_into_node(Page& header, Page& parent,
                                               int left_index, Page& right,
                                               int64_t key);
    [[nodiscard]] static bool insert_into_leaf_after_splitting(
        Page& header, Page& leaf, const page_data_t& record);
    [[nodiscard]] static bool insert_into_node_after_splitting(
        Page& header, Page& old, int left_index, Page& right, int64_t key);

    // delete operation helper methods
    [[nodiscard]] static bool delete_entry(Page& header, Page& node,
                                           int64_t key);
    static void remove_branch_from_internal(Page& header, Page& node,
                                            int64_t key);
    static void remove_record_from_leaf(Page& header, Page& node, int64_t key);
    [[nodiscard]] static bool adjust_root(Page& header, Page& root);
    [[nodiscard]] static bool coalesce_nodes(Page& header, Page& parent,
                                             Page& left, Page& right,
                                             int64_t k_prime);
    [[nodiscard]] static bool redistribute_nodes(Page& header, Page& parent,
                                                 Page& left, Page& right,
                                                 int k_prime_index,
                                                 int64_t k_prime);
};

#endif  // BPT_H_
