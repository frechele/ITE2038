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
    [[nodiscard]] static bool initialize(int num_buf);
    [[nodiscard]] static bool shutdown();

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
    [[nodiscard]] static table_page_t make_node(table_id_t table_id, bool is_leaf);
    [[nodiscard]] static table_page_t find_leaf(table_id_t table_id, int64_t key);
    [[nodiscard]] static int path_to_root(table_id_t table_id, pagenum_t child);

    // insert operation helper methods
    [[nodiscard]] static bool insert_into_leaf(table_id_t table_id, const table_page_t& leaf_tpid,
                                               const page_data_t& record);
    [[nodiscard]] static bool insert_into_parent(table_id_t table_id, const table_page_t& left_tpid,
                                                 const table_page_t& right_tpid, int64_t key);
    [[nodiscard]] static bool insert_into_new_root(table_id_t table_id, const table_page_t& left_tpid,
                                                   const table_page_t& right_tpid, int64_t key);
    [[nodiscard]] static bool insert_into_node(table_id_t table_id, const table_page_t& parent_tpid,
                                               int left_index, const table_page_t& right_tpid,
                                               int64_t key);
    [[nodiscard]] static bool insert_into_leaf_after_splitting(table_id_t table_id, 
        const table_page_t& leaf_tpid, const page_data_t& record);
    [[nodiscard]] static bool insert_into_node_after_splitting(table_id_t table_id, 
        const table_page_t& old_tpid, int left_index, const table_page_t& right_tpid, int64_t key);

    // delete operation helper methods
    [[nodiscard]] static bool delete_entry(table_id_t table_id, const table_page_t& node_tpid,
                                           int64_t key);
    static void remove_branch_from_internal(Page& node,
                                            int64_t key);
    static void remove_record_from_leaf(Page& node, int64_t key);
    [[nodiscard]] static bool adjust_root(table_id_t table_id, const table_page_t& root_tpid);
    [[nodiscard]] static bool coalesce_nodes(table_id_t table_id, const table_page_t& parent_tpid,
                                             const table_page_t& left_tpid, const table_page_t& right_tpid,
                                             int64_t k_prime);
    [[nodiscard]] static bool redistribute_nodes(table_id_t table_id, const table_page_t& parent_tpid,
                                                 const table_page_t& left_tpid, const table_page_t& right_tpid,
                                                 int k_prime_index,
                                                 int64_t k_prime);
};

#endif  // BPT_H_
