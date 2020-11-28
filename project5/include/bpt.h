#ifndef BPT_H_
#define BPT_H_

#include "page.h"
#include "table.h"
#include "xact.h"

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

    [[nodiscard]] static bool open_table(Table& table);
    [[nodiscard]] static bool close_table(Table& table);

    [[nodiscard]] static bool insert(Table& table, const page_data_t& record);
    [[nodiscard]] static bool remove(Table& table, int64_t key);
    [[nodiscard]] static std::optional<page_data_t> find(Table& table,
                                                         int64_t key,
                                                         Xact* xact = nullptr);
    [[nodiscard]] static bool update(Table& table, int64_t key,
                                     const char* value, Xact* xact = nullptr);

 private:
    [[nodiscard]] static pagenum_t make_node(Table& table, bool is_leaf);
    [[nodiscard]] static pagenum_t find_leaf(Table& table, int64_t key);
    [[nodiscard]] static int path_to_root(Table& table, pagenum_t child);

    // insert operation helper methods
    [[nodiscard]] static bool insert_into_leaf(Table& table, pagenum_t leaf,
                                               const page_data_t& record);
    [[nodiscard]] static bool insert_into_parent(Table& table, pagenum_t left,
                                                 pagenum_t right, int64_t key);
    [[nodiscard]] static bool insert_into_new_root(Table& table, pagenum_t left,
                                                   pagenum_t right,
                                                   int64_t key);
    [[nodiscard]] static bool insert_into_node(Table& table, pagenum_t parent,
                                               int left_index, pagenum_t right,
                                               int64_t key);
    [[nodiscard]] static bool insert_into_leaf_after_splitting(
        Table& table, pagenum_t leaf, const page_data_t& record);
    [[nodiscard]] static bool insert_into_node_after_splitting(Table& table,
                                                               pagenum_t old,
                                                               int left_index,
                                                               pagenum_t right,
                                                               int64_t key);

    // delete operation helper methods
    [[nodiscard]] static bool delete_entry(Table& table, pagenum_t node,
                                           int64_t key);
    static void remove_branch_from_internal(Page& node, int64_t key);
    static void remove_record_from_leaf(Page& node, int64_t key);
    [[nodiscard]] static bool adjust_root(Table& table, pagenum_t root);
    [[nodiscard]] static bool coalesce_nodes(Table& table, pagenum_t parent,
                                             pagenum_t left, pagenum_t right,
                                             int64_t k_prime);
    [[nodiscard]] static bool redistribute_nodes(Table& table, pagenum_t parent,
                                                 pagenum_t left,
                                                 pagenum_t right,
                                                 int k_prime_index,
                                                 int64_t k_prime);
    [[nodiscard]] static bool redistribute_nodes_to_left(
        Table& table, Page& parent, Page& left, Page& right, int k_prime_index,
        int64_t k_prime);
    [[nodiscard]] static bool redistribute_nodes_to_right(
        Table& table, Page& parent, Page& left, Page& right, int k_prime_index,
        int64_t k_prime);
};

#endif  // BPT_H_
