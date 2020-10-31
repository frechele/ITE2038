#include "bpt.h"

#include "buffer.h"
#include "common.h"
#include "dbms.h"
#include "file.h"

#include <cassert>

#include <algorithm>
#include <array>
#include <queue>
#include <sstream>

namespace
{
constexpr int cut(int length)
{
    return (length % 2) ? (length / 2 + 1) : (length / 2);
}

int get_left_index(const Page& parent, pagenum_t left_num)
{
    if (parent.header().page_a_number == left_num)
        return 0;

    const int num_keys = parent.header().num_keys;
    auto branches = parent.branches();

    int left_index = 0;
    while (left_index < num_keys &&
           branches[left_index].child_page_number != left_num)
        ++left_index;

    return left_index + 1;
}

int get_neighbor_index(const Page& parent, const table_page_t& node_tpid)
{
    const pagenum_t pagenum = PAGENUM(node_tpid);
    const int num_keys = parent.header().num_keys;
    const auto branches = parent.branches();
    for (int i = -1; i < num_keys; ++i)
    {
        const pagenum_t j = (i == -1) ? parent.header().page_a_number
                                      : branches[i].child_page_number;

        if (j == pagenum)
            return i;
    }

    return num_keys;
}

template <typename T>
int binary_search_key(T* data, int size, int64_t key)
{
    const auto end = data + size;

    auto it = std::lower_bound(data, end, key, [](const auto& lhs, auto rhs) {
        return lhs.key < rhs;
    });

    if (it == end || (*it).key != key)
        return size;

    return std::distance(data, it);
}
}  // namespace

int BPTree::open_table(const std::string& filename)
{
    if (auto opt = TblMgr().open_table(filename); opt.has_value())
        return opt.value();

    return -1;
}

bool BPTree::close_table(table_id_t table_id)
{
    return BufMgr().close_table(table_id);
}

bool BPTree::is_open(table_id_t table_id)
{
    return TblMgr().is_open(table_id);
}

bool BPTree::insert(table_id_t table_id, const page_data_t& record)
{
    // case 1 : duplicated key
    if (find(table_id, record.key))
        return false;

    pagenum_t root_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& header) {
            root_page_number = header.header_page().root_page_number;

            return true;
        },
        table_id));

    // case 2 : tree does not exist
    if (root_page_number == NULL_PAGE_NUM)
    {
        const auto new_node_tpid = make_node(table_id, true);
        CHECK_FAILURE(PAGENUM(new_node_tpid) != NULL_PAGE_NUM);

        CHECK_FAILURE(buffer(
            [&](Page& header) {
                header.header_page().root_page_number = PAGENUM(new_node_tpid);

                header.mark_dirty();

                return true;
            },
            table_id));

        return buffer(
            [&](Page& new_page) {
                new_page.header().parent_page_number = NULL_PAGE_NUM;
                new_page.header().num_keys = 1;

                new_page.data()[0] = record;

                new_page.mark_dirty();

                return true;
            },
            new_node_tpid);
    }

    const table_page_t leaf_tpid = find_leaf(table_id, record.key);

    int leaf_num_keys = 0;
    CHECK_FAILURE(buffer(
        [&](Page& leaf) {
            leaf_num_keys = leaf.header().num_keys;

            return true;
        },
        leaf_tpid));

    // case 3-1 : leaf has room for key
    if (leaf_num_keys < LEAF_ORDER - 1)
    {
        return insert_into_leaf(table_id, leaf_tpid, record);
    }

    return insert_into_leaf_after_splitting(table_id, leaf_tpid, record);
}

bool BPTree::remove(table_id_t table_id, int64_t key)
{
    auto record = find(table_id, key);
    CHECK_FAILURE(record);

    table_page_t leaf_tpid = find_leaf(table_id, key);
    CHECK_FAILURE(PAGENUM(leaf_tpid) != NULL_PAGE_NUM);

    return delete_entry(table_id, leaf_tpid, key);
}

std::optional<page_data_t> BPTree::find(table_id_t table_id, int64_t key)
{
    table_page_t tpid = find_leaf(table_id, key);
    CHECK_FAILURE2(PAGENUM(tpid) != NULL_PAGE_NUM, std::nullopt);

    std::optional<page_data_t> result{ std::nullopt };

    CHECK_FAILURE2(buffer(
                       [&](Page& page) {
                           const int num_keys = page.header().num_keys;
                           int i =
                               binary_search_key(page.data(), num_keys, key);

                           CHECK_FAILURE(i != num_keys);

                           result = page.data()[i];

                           return true;
                       },
                       tpid), std::nullopt);

    return result;
}

std::vector<page_data_t> BPTree::find_range(table_id_t table_id,
                                            int64_t key_start, int64_t key_end)
{
    std::vector<page_data_t> result;

    CHECK_FAILURE2(
        buffer(
            [&](Page& header) {
                pagenum_t pagenum = std::get<1>(find_leaf(table_id, key_start));
                CHECK_FAILURE(pagenum != NULL_PAGE_NUM);

                int i = 0;
                CHECK_FAILURE(buffer(
                    [&](Page& page) {
                        const int num_keys = page.header().num_keys;
                        const auto data = page.data();

                        for (; i < num_keys && data[i].key < key_start; ++i)
                            ;

                        return (i != num_keys);
                    },
                    table_id, pagenum));

                while (pagenum != NULL_PAGE_NUM)
                {
                    CHECK_FAILURE(buffer(
                        [&](Page& page) {
                            const int num_keys = page.header().num_keys;
                            const auto data = page.data();

                            for (; i < num_keys && data[i].key <= key_end; ++i)
                                result.push_back(data[i]);
                            i = 0;

                            pagenum = page.header().page_a_number;

                            return true;
                        },
                        table_id, pagenum));
                }

                return true;
            },
            table_id),
        {});

    return result;
}

std::string BPTree::to_string(table_id_t table_id)
{
    std::stringstream ss;

    CHECK_FAILURE2(
        buffer(
            [&](Page& header) {
                if (header.header_page().root_page_number != NULL_PAGE_NUM)
                {
                    int rank = 0;
                    std::queue<pagenum_t> queue;
                    queue.emplace(header.header_page().root_page_number);

                    while (!queue.empty())
                    {
                        const pagenum_t pagenum = queue.front();
                        queue.pop();

                        CHECK_FAILURE(buffer(
                            [&](Page& page) {
                                if (page.header().page_a_number !=
                                    NULL_PAGE_NUM)
                                {
                                    CHECK_FAILURE(buffer(
                                        [&](Page& parent) {
                                            if (parent.header().page_a_number ==
                                                pagenum)  // leftmost
                                            {
                                                const int new_rank =
                                                    path_to_root(table_id,
                                                                 pagenum);
                                                if (new_rank != rank)
                                                {
                                                    rank = new_rank;
                                                    ss << '\n';
                                                }
                                            }

                                            return true;
                                        },
                                        table_id,
                                        page.header().parent_page_number));
                                }

                                const int num_keys = page.header().num_keys;
                                const int is_leaf = page.header().is_leaf;
                                const pagenum_t page_a_number =
                                    page.header().page_a_number;
                                const auto data = page.data();
                                const auto branches = page.branches();

                                for (int i = 0; i < num_keys; ++i)
                                    ss << (is_leaf ? data[i].key
                                                   : branches[i].key)
                                       << ' ';

                                if (!is_leaf && page_a_number != NULL_PAGE_NUM)
                                {
                                    queue.emplace(page_a_number);
                                    for (int i = 0; i < num_keys; ++i)
                                        queue.emplace(
                                            branches[i].child_page_number);
                                }

                                ss << "| ";

                                return true;
                            },
                            table_id, pagenum));
                    }
                }

                return true;
            },
            table_id),
        "");

    return ss.str();
}

table_page_t BPTree::make_node(table_id_t table_id, bool is_leaf)
{
    pagenum_t pagenum;
    CHECK_FAILURE2(TblMgr().get(table_id).file_alloc_page(pagenum),
                   std::make_pair(table_id, NULL_PAGE_NUM));

    CHECK_FAILURE2(buffer(
                       [&](Page& page) {
                           page.clear();

                           page.header().is_leaf = is_leaf;

                           page.mark_dirty();

                           return true;
                       },
                       table_id, pagenum),
                   std::make_pair(table_id, NULL_PAGE_NUM));

    return { table_id, pagenum };
}

table_page_t BPTree::find_leaf(table_id_t table_id, int64_t key)
{
    pagenum_t root_page_number;
    CHECK_FAILURE2(buffer(
                       [&](Page& header) {
                           root_page_number =
                               header.header_page().root_page_number;

                           return true;
                       },
                       table_id),
                   std::make_pair(table_id, NULL_PAGE_NUM));

    if (root_page_number == NULL_PAGE_NUM)
        return { table_id, NULL_PAGE_NUM };

    pagenum_t current_num = root_page_number;

    bool run = true;
    while (run)
    {
        CHECK_FAILURE2(
            buffer(
                [&](Page& current) {
                    if (current.header().is_leaf)
                    {
                        run = false;
                        return true;
                    }

                    const auto branches = current.branches();

                    int child_idx = std::distance(
                        branches,
                        std::upper_bound(branches,
                                         branches + current.header().num_keys,
                                         key, [](auto lhs, const auto& rhs) {
                                             return lhs < rhs.key;
                                         }));

                    --child_idx;

                    current_num = (child_idx == -1)
                                      ? current.header().page_a_number
                                      : branches[child_idx].child_page_number;

                    return true;
                },
                table_id, current_num),
            std::make_pair(table_id, NULL_PAGE_NUM));
    }

    return { table_id, current_num };
}

int BPTree::path_to_root(table_id_t table_id, pagenum_t child_num)
{
    int length = 0;

    pagenum_t root_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& header) {
            root_page_number = header.header_page().root_page_number;

            return true;
        },
        table_id));

    const pagenum_t root_page = root_page_number;
    while (child_num != root_page)
    {
        CHECK_FAILURE2(buffer(
                           [&](Page& child) {
                               child_num = child.header().parent_page_number;

                               return true;
                           },
                           table_id, child_num),
                       -1);

        ++length;
    }

    return length;
}

bool BPTree::insert_into_leaf(table_id_t table_id,
                              const table_page_t& leaf_tpid,
                              const page_data_t& record)
{
    return buffer(
        [&](Page& leaf) {
            const int num_keys = leaf.header().num_keys;
            auto data = leaf.data();

            const int insertion_point = std::distance(
                data, std::lower_bound(data, data + num_keys, record.key,
                                       [](const auto& lhs, auto rhs) {
                                           return lhs.key < rhs;
                                       }));

            for (int i = num_keys; i > insertion_point; --i)
                data[i] = data[i - 1];

            data[insertion_point] = record;
            ++leaf.header().num_keys;

            leaf.mark_dirty();

            return true;
        },
        leaf_tpid);
}

bool BPTree::insert_into_parent(table_id_t table_id,
                                const table_page_t& left_tpid,
                                const table_page_t& right_tpid, int64_t key)
{
    table_page_t parent_tpid;
    CHECK_FAILURE(buffer(
        [&](Page& left) {
            parent_tpid =
                std::make_pair(table_id, left.header().parent_page_number);

            return true;
        },
        left_tpid));

    // case 1 : new root
    if (PAGENUM(parent_tpid) == NULL_PAGE_NUM)
    {
        return insert_into_new_root(table_id, left_tpid, right_tpid, key);
    }

    // case 2 : leaf or node
    int num_keys, left_index;
    CHECK_FAILURE(buffer(
        [&](Page& parent) {
            num_keys = parent.header().num_keys;
            left_index = get_left_index(parent, PAGENUM(left_tpid));

            return true;
        },
        parent_tpid));

    // case 2-1 : the new key fits into the node
    if (num_keys < INTERNAL_ORDER - 1)
    {
        return insert_into_node(table_id, parent_tpid, left_index, right_tpid,
                                key);
    }

    // case 2-2 : split a node in order to preserve the B+ tree
    // properties
    return insert_into_node_after_splitting(table_id, parent_tpid, left_index,
                                            right_tpid, key);
}

bool BPTree::insert_into_new_root(table_id_t table_id,
                                  const table_page_t& left_tpid,
                                  const table_page_t& right_tpid, int64_t key)
{
    table_page_t new_root_tpid = make_node(table_id, false);
    CHECK_FAILURE(PAGENUM(new_root_tpid) != NULL_PAGE_NUM);

    CHECK_FAILURE(buffer(
        [&](Page& new_root) {
            new_root.header().num_keys = 1;
            new_root.header().page_a_number = PAGENUM(left_tpid);

            new_root.branches()[0].key = key;
            new_root.branches()[0].child_page_number = PAGENUM(right_tpid);

            new_root.mark_dirty();

            return true;
        },
        new_root_tpid));

    CHECK_FAILURE(buffer(
        [&](Page& left) {
            left.header().parent_page_number = PAGENUM(new_root_tpid);

            left.mark_dirty();

            return true;
        },
        left_tpid));

    CHECK_FAILURE(buffer(
        [&](Page& right) {
            right.header().parent_page_number = PAGENUM(new_root_tpid);

            right.mark_dirty();

            return true;
        },
        right_tpid));

    return buffer(
        [&](Page& header) {
            header.header_page().root_page_number = PAGENUM(new_root_tpid);

            header.mark_dirty();

            return true;
        },
        table_id);
}

bool BPTree::insert_into_node(table_id_t table_id,
                              const table_page_t& parent_tpid, int left_index,
                              const table_page_t& right_tpid, int64_t key)
{
    return buffer(
        [&](Page& parent) {
            const int num_keys = parent.header().num_keys;
            auto branches = parent.branches();

            for (int i = num_keys; i > left_index; --i)
            {
                branches[i] = branches[i - 1];
            }
            branches[left_index].child_page_number = PAGENUM(right_tpid);
            branches[left_index].key = key;
            ++parent.header().num_keys;

            parent.mark_dirty();

            return true;
        },
        parent_tpid);
}

bool BPTree::insert_into_leaf_after_splitting(table_id_t table_id,
                                              const table_page_t& leaf_tpid,
                                              const page_data_t& record)
{
    std::array<page_data_t, LEAF_ORDER> temp_data;
    const int split_pivot = cut(LEAF_ORDER - 1);

    const table_page_t new_leaf_tpid = make_node(table_id, true);
    CHECK_FAILURE(PAGENUM(new_leaf_tpid) != NULL_PAGE_NUM);

    pagenum_t parent_page_number, page_a_number;
    CHECK_FAILURE(buffer(
        [&](Page& leaf) {
            parent_page_number = leaf.header().parent_page_number;
            page_a_number = leaf.header().page_a_number;

            const int num_keys = leaf.header().num_keys;
            const auto data = leaf.data();

            const int insertion_point = std::distance(
                data,
                std::lower_bound(
                    data, data + (LEAF_ORDER - 1), record.key,
                    [](const auto& lhs, auto rhs) { return lhs.key < rhs; }));

            for (int i = 0, j = 0; i < num_keys; ++i, ++j)
            {
                if (j == insertion_point)
                    ++j;
                temp_data[j] = data[i];
            }

            temp_data[insertion_point] = record;

            leaf.header().num_keys = 0;

            for (int i = 0; i < split_pivot; ++i)
            {
                data[i] = temp_data[i];
                ++leaf.header().num_keys;
            }

            leaf.header().page_a_number = PAGENUM(new_leaf_tpid);

            leaf.mark_dirty();

            return true;
        },
        leaf_tpid));

    int64_t insert_key;
    CHECK_FAILURE(buffer(
        [&](Page& new_leaf) {
            auto new_data = new_leaf.data();

            for (int i = split_pivot, j = 0; i < LEAF_ORDER; ++i, ++j)
            {
                new_data[j] = temp_data[i];
                ++new_leaf.header().num_keys;
            }

            new_leaf.header().page_a_number = page_a_number;

            new_leaf.header().parent_page_number = parent_page_number;

            insert_key = new_data[0].key;

            new_leaf.mark_dirty();

            return true;
        },
        new_leaf_tpid));

    return insert_into_parent(table_id, leaf_tpid, new_leaf_tpid, insert_key);
}

bool BPTree::insert_into_node_after_splitting(table_id_t table_id,
                                              const table_page_t& old_tpid,
                                              int left_index,
                                              const table_page_t& right_tpid,
                                              int64_t key)
{
    std::array<page_branch_t, INTERNAL_ORDER> temp_data;
    const int split_pivot = cut(INTERNAL_ORDER);

    const table_page_t new_page_tpid = make_node(table_id, false);
    CHECK_FAILURE(PAGENUM(new_page_tpid) != NULL_PAGE_NUM);

    pagenum_t parent_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& old) {
            const int num_keys = old.header().num_keys;
            auto branches = old.branches();
            for (int i = 0, j = 0; i < num_keys; ++i, ++j)
            {
                if (j == left_index)
                    ++j;
                temp_data[j] = branches[i];
            }
            temp_data[left_index].key = key;
            temp_data[left_index].child_page_number = PAGENUM(right_tpid);

            old.header().num_keys = 0;
            for (int i = 0; i < split_pivot - 1; ++i)
            {
                branches[i] = temp_data[i];
                ++old.header().num_keys;
            }

            parent_page_number = old.header().parent_page_number;

            old.mark_dirty();

            return true;
        },
        old_tpid));

    int64_t k_prime;
    CHECK_FAILURE(buffer(
        [&](Page& new_page) {
            auto new_branches = new_page.branches();

            k_prime = temp_data[split_pivot - 1].key;
            new_page.header().page_a_number =
                temp_data[split_pivot - 1].child_page_number;
            for (int i = split_pivot, j = 0; i < INTERNAL_ORDER; ++i, ++j)
            {
                new_branches[j] = temp_data[i];
                ++new_page.header().num_keys;
            }
            new_page.header().parent_page_number = parent_page_number;

            const int new_keys = new_page.header().num_keys;
            for (int i = -1; i < new_keys; ++i)
            {
                const pagenum_t j = (i == -1)
                                        ? new_page.header().page_a_number
                                        : new_branches[i].child_page_number;

                CHECK_FAILURE(buffer(
                    [&](Page& child) {
                        child.header().parent_page_number = new_page.pagenum();

                        child.mark_dirty();

                        return true;
                    },
                    table_id, j));
            }

            new_page.mark_dirty();

            return true;
        },
        new_page_tpid));

    return insert_into_parent(table_id, old_tpid, new_page_tpid, k_prime);
}

bool BPTree::delete_entry(table_id_t table_id, const table_page_t& node_tpid,
                          int64_t key)
{
    table_page_t parent_tpid;
    int node_num_keys, is_leaf;
    CHECK_FAILURE(buffer(
        [&](Page& node) {
            is_leaf = node.header().is_leaf;

            if (is_leaf)
                remove_record_from_leaf(node, key);
            else
                remove_branch_from_internal(node, key);

            node.mark_dirty();

            node_num_keys = node.header().num_keys;
            parent_tpid = { table_id, node.header().parent_page_number };

            return true;
        },
        node_tpid));

    pagenum_t root_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& header) {
            root_page_number = header.header_page().root_page_number;

            return true;
        },
        table_id));

    if (root_page_number == PAGENUM(node_tpid))
        return adjust_root(table_id, node_tpid);

    if (node_num_keys > MERGE_THRESHOLD)
        return true;

    int neighbor_index, k_prime_index;
    int64_t k_prime;
    table_page_t left_tpid;
    CHECK_FAILURE(buffer(
        [&](Page& parent) {
            neighbor_index = get_neighbor_index(parent, node_tpid);
            k_prime_index = (neighbor_index == -1) ? 0 : neighbor_index;
            k_prime = parent.branches()[k_prime_index].key;

            left_tpid = { table_id,
                          (neighbor_index == -1)
                              ? parent.branches()[0].child_page_number
                              : ((neighbor_index == 0)
                                     ? parent.header().page_a_number
                                     : parent.branches()[neighbor_index - 1]
                                           .child_page_number) };

            return true;
        },
        parent_tpid));

    const table_page_t* left_ptr = &left_tpid;
    const table_page_t* right_ptr = &node_tpid;

    if (neighbor_index == -1)
    {
        std::swap(left_ptr, right_ptr);
    }

    const int capacity = is_leaf ? LEAF_ORDER : INTERNAL_ORDER - 1;
    int left_num_keys;
    CHECK_FAILURE(buffer(
        [&](Page& left) {
            left_num_keys = left.header().num_keys;

            return true;
        },
        left_tpid));

    if (left_num_keys + node_num_keys < capacity)
        return coalesce_nodes(table_id, parent_tpid, *left_ptr, *right_ptr,
                              k_prime);

    return redistribute_nodes(table_id, parent_tpid, *left_ptr, *right_ptr,
                              k_prime_index, k_prime);
}

void BPTree::remove_branch_from_internal(Page& node, int64_t key)
{
    const int num_keys = node.header().num_keys;
    auto branches = node.branches();

    int i = binary_search_key(branches, num_keys, key);
    for (++i; i < num_keys; ++i)
        branches[i - 1] = branches[i];

    --node.header().num_keys;
}

void BPTree::remove_record_from_leaf(Page& node, int64_t key)
{
    const int num_keys = node.header().num_keys;
    auto data = node.data();

    int i = binary_search_key(data, num_keys, key);
    for (++i; i < num_keys; ++i)
        data[i - 1] = data[i];

    --node.header().num_keys;
}

bool BPTree::adjust_root(table_id_t table_id, const table_page_t& root_tpid)
{
    int num_keys, is_leaf;
    pagenum_t page_a_number;
    CHECK_FAILURE(buffer(
        [&](Page& root) {
            num_keys = root.header().num_keys;
            is_leaf = root.header().is_leaf;

            page_a_number = root.header().page_a_number;

            return true;
        },
        root_tpid));

    if (num_keys > 0)
        return true;

    pagenum_t new_root_page_number;
    if (is_leaf)
    {
        new_root_page_number = NULL_PAGE_NUM;
    }
    else
    {
        new_root_page_number = page_a_number;

        CHECK_FAILURE(buffer(
            [&](Page& new_root) {
                new_root.header().parent_page_number = 0;

                new_root.mark_dirty();

                return true;
            },
            table_id, new_root_page_number));
    }

    CHECK_FAILURE(buffer(
        [&](Page& header) {
            header.header_page().root_page_number = new_root_page_number;

            header.mark_dirty();

            return true;
        },
        table_id));

    return TblMgr().get(table_id).file_free_page(PAGENUM(root_tpid));
}

bool BPTree::coalesce_nodes(table_id_t table_id,
                            const table_page_t& parent_tpid,
                            const table_page_t& left_tpid,
                            const table_page_t& right_tpid, int64_t k_prime)
{
    CHECK_FAILURE(buffer(
        [&](Page& left) {
            return buffer(
                [&](Page& right) {
                    const int insertion_index = left.header().num_keys;

                    if (right.header().is_leaf)
                    {
                        auto left_data = left.data();
                        auto right_data = right.data();

                        const int num_keys = right.header().num_keys;
                        for (int i = insertion_index, j = 0; j < num_keys;
                             ++i, ++j)
                        {
                            left_data[i] = right_data[j];
                            ++left.header().num_keys;
                            --right.header().num_keys;
                        }

                        left.header().page_a_number =
                            right.header().page_a_number;
                    }
                    else
                    {
                        auto left_branches = left.branches();
                        auto right_branches = right.branches();

                        const int n_end = right.header().num_keys;
                        const pagenum_t left_number = left.pagenum();

                        for (int i = insertion_index, j = -1; j < n_end;
                             ++i, ++j)
                        {
                            if (j == -1)
                            {
                                left_branches[i].key = k_prime;
                                left_branches[i].child_page_number =
                                    right.header().page_a_number;
                            }
                            else
                            {
                                left_branches[i] = right_branches[j];
                            }

                            CHECK_FAILURE(buffer(
                                [&](Page& tmp) {
                                    tmp.header().parent_page_number =
                                        left_number;

                                    tmp.mark_dirty();

                                    return true;
                                },
                                table_id, left_branches[i].child_page_number));

                            ++left.header().num_keys;
                            --right.header().num_keys;
                        }
                    }

                    left.mark_dirty();

                    return true;
                },
                right_tpid);
        },
        left_tpid));

    CHECK_FAILURE(delete_entry(table_id, parent_tpid, k_prime));

    return TblMgr().get(table_id).file_free_page(PAGENUM(right_tpid));
}

bool BPTree::redistribute_nodes(table_id_t table_id,
                                const table_page_t& parent_tpid,
                                const table_page_t& left_tpid,
                                const table_page_t& right_tpid,
                                int k_prime_index, int64_t k_prime)
{
    return buffer(
        [&](Page& parent) {
            return buffer(
                [&](Page& left) {
                    return buffer(
                        [&](Page& right) {
                            const int left_num_key = left.header().num_keys;
                            const int right_num_key = right.header().num_keys;

                            if (left_num_key < right_num_key)
                            {
                                if (left.header().is_leaf)
                                {
                                    auto left_data = left.data();
                                    auto right_data = right.data();

                                    left_data[left_num_key] = right_data[0];
                                    parent.branches()[k_prime_index].key =
                                        right_data[1].key;

                                    for (int i = 0; i < right_num_key - 1; ++i)
                                        right_data[i] = right_data[i + 1];
                                }
                                else
                                {
                                    auto left_branches = left.branches();
                                    auto right_branches = right.branches();

                                    left_branches[left_num_key].key = k_prime;
                                    const pagenum_t moved_child_pagenum =
                                        left_branches[left_num_key]
                                            .child_page_number =
                                            right.header().page_a_number;

                                    CHECK_FAILURE(buffer(
                                        [&](Page& tmp) {
                                            tmp.header().parent_page_number =
                                                left.pagenum();

                                            tmp.mark_dirty();

                                            return true;
                                        },
                                        table_id, moved_child_pagenum));

                                    parent.branches()[k_prime_index].key =
                                        right_branches[0].key;

                                    right.header().page_a_number =
                                        right_branches[0].child_page_number;
                                    for (int i = 0; i < right_num_key - 1; ++i)
                                        right_branches[i] =
                                            right_branches[i + 1];
                                }

                                ++left.header().num_keys;
                                --right.header().num_keys;
                            }
                            else
                            {
                                if (left.header().is_leaf)
                                {
                                    auto left_data = left.data();
                                    auto right_data = right.data();

                                    for (int i = right_num_key; i > 0; --i)
                                        right_data[i] = right_data[i - 1];

                                    right_data[0] = left_data[left_num_key - 1];
                                    parent.branches()[k_prime_index].key =
                                        right_data[0].key;
                                }
                                else
                                {
                                    auto left_branches = left.branches();
                                    auto right_branches = right.branches();

                                    for (int i = right_num_key; i > 0; --i)
                                        right_branches[i] =
                                            right_branches[i - 1];

                                    right_branches[0].key = k_prime;
                                    const pagenum_t moved_child_pagenum =
                                        right_branches[0].child_page_number =
                                            right.header().page_a_number;

                                    CHECK_FAILURE(buffer(
                                        [&](Page& tmp) {
                                            tmp.header().parent_page_number =
                                                right.pagenum();

                                            tmp.mark_dirty();

                                            return true;
                                        },
                                        table_id, moved_child_pagenum));

                                    parent.branches()[k_prime_index].key =
                                        left_branches[left_num_key - 1].key;

                                    right.header().page_a_number =
                                        left_branches[left_num_key - 1]
                                            .child_page_number;
                                }

                                --left.header().num_keys;
                                ++right.header().num_keys;
                            }

                            left.mark_dirty();
                            right.mark_dirty();
                            parent.mark_dirty();

                            return true;
                        },
                        right_tpid);
                },
                left_tpid);
        },
        parent_tpid);
}
