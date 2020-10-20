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
    static BPTree& get();

    [[nodiscard]] int open_table(const std::string& filename);
    [[nodiscard]] bool close_table(int table_id);
    [[nodiscard]] bool is_open(int table_id) const;

    [[nodiscard]] bool insert(int table_id, const page_data_t& record);
    [[nodiscard]] bool remove(int table_id, int64_t key);
    [[nodiscard]] std::optional<page_data_t> find(int table_id,
                                                  int64_t key) const;
    [[nodiscard]] std::vector<page_data_t> find_range(int table_id,
                                                      int64_t key_start,
                                                      int64_t key_end) const;

    [[nodiscard]] std::string to_string(int table_id) const;

 private:
    [[nodiscard]] std::optional<Page> make_node(Page& header,
                                                bool is_leaf) const;

    [[nodiscard]] std::optional<page_data_t> find(Page& header,
                                                  int64_t key) const;
    [[nodiscard]] std::optional<Page> find_leaf(Page& header,
                                                int64_t key) const;
    [[nodiscard]] int path_to_root(Page& header, pagenum_t child) const;

    // insert operation helper methods
    [[nodiscard]] bool insert_into_leaf(Page& header, Page& leaf,
                                        const page_data_t& record);
    [[nodiscard]] bool insert_into_parent(Page& header, Page& left, Page& right,
                                          int64_t key);
    [[nodiscard]] bool insert_into_new_root(Page& header, Page& left,
                                            Page& right, int64_t key);
    [[nodiscard]] bool insert_into_node(Page& header, Page& parent,
                                        int left_index, Page& right,
                                        int64_t key);
    [[nodiscard]] bool insert_into_leaf_after_splitting(
        Page& header, Page& leaf, const page_data_t& record);
    [[nodiscard]] bool insert_into_node_after_splitting(Page& header, Page& old,
                                                        int left_index,
                                                        Page& right,
                                                        int64_t key);

    // delete operation helper methods
    [[nodiscard]] bool delete_entry(Page& header, Page node, int64_t key);
    void remove_branch_from_internal(Page& header, Page& node, int64_t key);
    void remove_record_from_leaf(Page& header, Page& node, int64_t key);
    [[nodiscard]] bool adjust_root(Page& header, Page& root);
    [[nodiscard]] bool coalesce_nodes(Page& header, Page& parent, Page& left,
                                      Page& right, int64_t k_prime);
    [[nodiscard]] bool redistribute_nodes(Page& header, Page& parent,
                                          Page& left, Page& right,
                                          int k_prime_index, int64_t k_prime);
};

#endif  // BPT_H_
