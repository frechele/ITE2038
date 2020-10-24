#include "bpt.h"

#include "buffer.h"
#include "common.h"
#include "file.h"

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

int get_neighbor_index(const Page& parent, const Page& node)
{
    const pagenum_t pagenum = node.pagenum();
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

BPTree& BPTree::get()
{
    static BPTree instance;
    return instance;
}

int BPTree::open_table(const std::string& filename)
{
    if (auto opt = TableManager::get().open_table(filename); opt.has_value())
        return opt.value();

    return -1;
}

bool BPTree::close_table(TableID table_id)
{
    return TableManager::get().close_table(table_id);
}

bool BPTree::is_open(TableID table_id) const
{
    return TableManager::get().is_open(table_id);
}

bool BPTree::insert(TableID table_id, const page_data_t& record)
{
    return buffer(
        [&](Page& header) {
            // case 1 : duplicated key
            if (find(header, record.key))
                return false;

            // case 2 : tree does not exist
            if (header.header_page().root_page_number == NULL_PAGE_NUM)
            {
                auto [_, new_page_num] = make_node(header, true);
                CHECK_FAILURE(new_page_num != NULL_PAGE_NUM);

                return buffer(
                    [&](Page& new_page) {
                        new_page.header().parent_page_number = NULL_PAGE_NUM;
                        new_page.header().num_keys = 1;

                        new_page.data()[0] = record;

                        header.header_page().root_page_number =
                            new_page.pagenum();

                        new_page.mark_dirty();
                        header.mark_dirty();

                        return true;
                    },
                    table_id, new_page_num);
            }

            return buffer(
                [&](Page& leaf) {
                    // case 3-1 : leaf has room for key
                    if (leaf.header().num_keys < LEAF_ORDER - 1)
                    {
                        return insert_into_leaf(header, leaf, record);
                    }

                    // case 3-2 : leaf must be splitted
                    return insert_into_leaf_after_splitting(header, leaf,
                                                            record);
                },
                table_id, std::get<1>(find_leaf(header, record.key)));
        },
        table_id);
}

bool BPTree::remove(TableID table_id, int64_t key)
{
    return buffer(
        [&](Page& header) {
            auto record = find(header, key);
            CHECK_FAILURE(record);

            auto [_, leaf_num] = find_leaf(header, key);
            CHECK_FAILURE(leaf_num != NULL_PAGE_NUM);

            return buffer(
                [&](Page& leaf) { return delete_entry(header, leaf, key); },
                table_id, leaf_num);
        },
        table_id);
}

std::optional<page_data_t> BPTree::find(TableID table_id, int64_t key) const
{
    std::optional<page_data_t> result{ std::nullopt };

    CHECK_FAILURE2(buffer(
                       [&](Page& header) {
                           result = find(header, key);
                           return true;
                       },
                       table_id),
                   std::nullopt);

    return result;
}

std::vector<page_data_t> BPTree::find_range(TableID table_id, int64_t key_start,
                                            int64_t key_end) const
{
    std::vector<page_data_t> result;

    CHECK_FAILURE2(
        buffer(
            [&](Page& header) {
                pagenum_t pagenum = std::get<1>(find_leaf(header, key_start));
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

std::string BPTree::to_string(TableID table_id) const
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
                                                    path_to_root(header,
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

table_page_t BPTree::make_node(Page& header, bool is_leaf) const
{
    const TableID table_id = header.table_id();

    pagenum_t pagenum;
    CHECK_FAILURE2(TableManager::get(table_id).file_alloc_page(pagenum),
                   std::make_tuple(table_id, NULL_PAGE_NUM));

    CHECK_FAILURE2(buffer(
                       [&](Page& page) {
                           page.clear();

                           page.header().is_leaf = is_leaf;

                           page.mark_dirty();

                           return true;
                       },
                       table_id, pagenum),
                   std::make_tuple(table_id, NULL_PAGE_NUM));

    return { table_id, pagenum };
}

std::optional<page_data_t> BPTree::find(Page& header, int64_t key) const
{
    auto [table_id, pagenum] = find_leaf(header, key);
    CHECK_FAILURE2(pagenum != NULL_PAGE_NUM, std::nullopt);

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
                       table_id, pagenum),
                   std::nullopt);

    return result;
}

table_page_t BPTree::find_leaf(Page& header, int64_t key) const
{
    const TableID table_id = header.table_id();

    if (header.header_page().root_page_number == NULL_PAGE_NUM)
        return { table_id, NULL_PAGE_NUM };

    pagenum_t current_num = header.header_page().root_page_number;

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
            std::make_tuple(table_id, NULL_PAGE_NUM));
    }

    return { table_id, current_num };
}

int BPTree::path_to_root(Page& header, pagenum_t child_num) const
{
    int length = 0;

    const TableID table_id = header.table_id();
    const pagenum_t root_page = header.pagenum();
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

bool BPTree::insert_into_leaf(Page& header, Page& leaf,
                              const page_data_t& record)
{
    const int num_keys = leaf.header().num_keys;
    auto data = leaf.data();

    int insertion_point = std::distance(
        data, std::lower_bound(
                  data, data + num_keys, record.key,
                  [](const auto& lhs, auto rhs) { return lhs.key < rhs; }));

    for (int i = num_keys; i > insertion_point; --i)
        data[i] = data[i - 1];

    data[insertion_point] = record;
    ++leaf.header().num_keys;

    leaf.mark_dirty();

    return true;
}

bool BPTree::insert_into_parent(Page& header, Page& left, Page& right,
                                int64_t key)
{
    // case 1 : new root
    if (left.header().parent_page_number == NULL_PAGE_NUM)
    {
        return insert_into_new_root(header, left, right, key);
    }

    // case 2 : leaf or node
    return buffer(
        [&](Page& parent) {
            const int left_index = get_left_index(parent, left.pagenum());

            // case 2-1 : the new key fits into the node
            if (parent.header().num_keys < INTERNAL_ORDER - 1)
            {
                return insert_into_node(header, parent, left_index, right, key);
            }

            // case 2-2 : split a node in order to preserve the B+ tree
            // properties
            return insert_into_node_after_splitting(header, parent, left_index,
                                                    right, key);
        },
        header.table_id(), left.header().parent_page_number);
}

bool BPTree::insert_into_new_root(Page& header, Page& left, Page& right,
                                  int64_t key)
{
    auto [table_id, new_root_num] = make_node(header, false);
    CHECK_FAILURE(new_root_num != NULL_PAGE_NUM);

    return buffer(
        [&](Page& new_root) {
            new_root.header().num_keys = 1;
            new_root.header().page_a_number = left.pagenum();
            new_root.branches()[0].key = key;
            new_root.branches()[0].child_page_number = right.pagenum();

            left.header().parent_page_number = new_root.pagenum();
            right.header().parent_page_number = new_root.pagenum();

            new_root.mark_dirty();
            left.mark_dirty();
            right.mark_dirty();

            header.header_page().root_page_number = new_root.pagenum();

            header.mark_dirty();

            return true;
        },
        table_id, new_root_num);
}

bool BPTree::insert_into_node(Page& header, Page& parent, int left_index,
                              Page& right, int64_t key)
{
    const int num_keys = parent.header().num_keys;
    auto branches = parent.branches();

    for (int i = num_keys; i > left_index; --i)
    {
        branches[i] = branches[i - 1];
    }
    branches[left_index].child_page_number = right.pagenum();
    branches[left_index].key = key;
    ++parent.header().num_keys;

    parent.mark_dirty();

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(Page& header, Page& leaf,
                                              const page_data_t& record)
{
    const int num_keys = leaf.header().num_keys;
    const auto data = leaf.data();

    int insertion_point = std::distance(
        data, std::lower_bound(
                  data, data + (LEAF_ORDER - 1), record.key,
                  [](const auto& lhs, auto rhs) { return lhs.key < rhs; }));

    std::array<page_data_t, LEAF_ORDER> temp_data;
    for (int i = 0, j = 0; i < num_keys; ++i, ++j)
    {
        if (j == insertion_point)
            ++j;
        temp_data[j] = data[i];
    }

    temp_data[insertion_point] = record;

    leaf.header().num_keys = 0;

    const int split_pivot = cut(LEAF_ORDER - 1);
    for (int i = 0; i < split_pivot; ++i)
    {
        data[i] = temp_data[i];
        ++leaf.header().num_keys;
    }

    auto [table_id, new_leaf_num] = make_node(header, true);
    CHECK_FAILURE(new_leaf_num != NULL_PAGE_NUM);

    return buffer(
        [&](Page& new_leaf) {
            auto new_data = new_leaf.data();

            for (int i = split_pivot, j = 0; i < LEAF_ORDER; ++i, ++j)
            {
                new_data[j] = temp_data[i];
                ++new_leaf.header().num_keys;
            }

            new_leaf.header().page_a_number = leaf.header().page_a_number;
            leaf.header().page_a_number = new_leaf.pagenum();

            new_leaf.header().parent_page_number =
                leaf.header().parent_page_number;

            new_leaf.mark_dirty();
            leaf.mark_dirty();

            return insert_into_parent(header, leaf, new_leaf, new_data[0].key);
        },
        table_id, new_leaf_num);
}

bool BPTree::insert_into_node_after_splitting(Page& header, Page& old,
                                              int left_index, Page& right,
                                              int64_t key)
{
    const TableID table_id = header.table_id();

    std::array<page_branch_t, INTERNAL_ORDER> temp_data;

    const int num_keys = old.header().num_keys;
    auto branches = old.branches();
    for (int i = 0, j = 0; i < num_keys; ++i, ++j)
    {
        if (j == left_index)
            ++j;
        temp_data[j] = branches[i];
    }
    temp_data[left_index].key = key;
    temp_data[left_index].child_page_number = right.pagenum();

    const int split_pivot = cut(INTERNAL_ORDER);

    old.header().num_keys = 0;
    for (int i = 0; i < split_pivot - 1; ++i)
    {
        branches[i] = temp_data[i];
        ++old.header().num_keys;
    }

    auto [_, new_page_num] = make_node(header, false);
    CHECK_FAILURE(new_page_num != NULL_PAGE_NUM);

    return buffer(
        [&](Page& new_page) {
            auto new_branches = new_page.branches();

            const int64_t k_prime = temp_data[split_pivot - 1].key;
            new_page.header().page_a_number =
                temp_data[split_pivot - 1].child_page_number;
            for (int i = split_pivot, j = 0; i < INTERNAL_ORDER; ++i, ++j)
            {
                new_branches[j] = temp_data[i];
                ++new_page.header().num_keys;
            }
            new_page.header().parent_page_number =
                old.header().parent_page_number;

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

            old.mark_dirty();
            new_page.mark_dirty();

            return insert_into_parent(header, old, new_page, k_prime);
        },
        table_id, new_page_num);
}

bool BPTree::delete_entry(Page& header, Page& node, int64_t key)
{
    const TableID table_id = header.table_id();
    const int is_leaf = node.header().is_leaf;

    if (is_leaf)
        remove_record_from_leaf(header, node, key);
    else
        remove_branch_from_internal(header, node, key);

    node.mark_dirty();

    if (header.header_page().root_page_number == node.pagenum())
        return adjust_root(header, node);

    if (node.header().num_keys > MERGE_THRESHOLD)
        return true;

    return buffer(
        [&](Page& parent) {
            const int neighbor_index = get_neighbor_index(parent, node);
            const int k_prime_index =
                (neighbor_index == -1) ? 0 : neighbor_index;
            const int64_t k_prime = parent.branches()[k_prime_index].key;

            const int left_pagenum =
                (neighbor_index == -1)
                    ? parent.branches()[0].child_page_number
                    : ((neighbor_index == 0)
                           ? parent.header().page_a_number
                           : parent.branches()[neighbor_index - 1]
                                 .child_page_number);

            return buffer(
                [&](Page& left) {
                    Page* left_ptr = &left;
                    Page* right_ptr = &node;

                    if (neighbor_index == -1)
                    {
                        std::swap(left_ptr, right_ptr);
                    }

                    const int capacity =
                        is_leaf ? LEAF_ORDER : INTERNAL_ORDER - 1;
                    if (left.header().num_keys + node.header().num_keys <
                        capacity)
                        return coalesce_nodes(header, parent, *left_ptr,
                                              *right_ptr, k_prime);

                    return redistribute_nodes(header, parent, *left_ptr,
                                              *right_ptr, k_prime_index,
                                              k_prime);
                },
                table_id, left_pagenum);
        },
        table_id, node.header().parent_page_number);
}

void BPTree::remove_branch_from_internal(Page& header, Page& node, int64_t key)
{
    const int num_keys = node.header().num_keys;
    auto branches = node.branches();

    int i = binary_search_key(branches, num_keys, key);
    for (++i; i < num_keys; ++i)
        branches[i - 1] = branches[i];

    --node.header().num_keys;
}

void BPTree::remove_record_from_leaf(Page& header, Page& node, int64_t key)
{
    const int num_keys = node.header().num_keys;
    auto data = node.data();

    int i = binary_search_key(data, num_keys, key);
    for (++i; i < num_keys; ++i)
        data[i - 1] = data[i];

    --node.header().num_keys;
}

bool BPTree::adjust_root(Page& header, Page& root)
{
    if (root.header().num_keys > 0)
        return true;

    if (root.header().is_leaf)
    {
        header.header_page().root_page_number = NULL_PAGE_NUM;
    }
    else
    {
        header.header_page().root_page_number = root.header().page_a_number;

        CHECK_FAILURE(buffer(
            [&](Page& new_root) {
                new_root.header().parent_page_number = 0;

                new_root.mark_dirty();

                return true;
            },
            header.table_id(), header.header_page().root_page_number));
    }

    header.mark_dirty();

    return root.free();
}

bool BPTree::coalesce_nodes(Page& header, Page& parent, Page& left, Page& right,
                            int64_t k_prime)
{
    const TableID table_id = header.table_id();
    const int insertion_index = left.header().num_keys;

    if (right.header().is_leaf)
    {
        auto left_data = left.data();
        auto right_data = right.data();

        const int num_keys = right.header().num_keys;
        for (int i = insertion_index, j = 0; j < num_keys; ++i, ++j)
        {
            left_data[i] = right_data[j];
            ++left.header().num_keys;
            --right.header().num_keys;
        }

        left.header().page_a_number = right.header().page_a_number;
    }
    else
    {
        auto left_branches = left.branches();
        auto right_branches = right.branches();

        const int n_end = right.header().num_keys;
        const pagenum_t left_number = left.pagenum();

        for (int i = insertion_index, j = -1; j < n_end; ++i, ++j)
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
                    tmp.header().parent_page_number = left_number;

                    tmp.mark_dirty();

                    return true;
                },
                table_id, left_branches[i].child_page_number));

            ++left.header().num_keys;
            --right.header().num_keys;
        }
    }

    left.mark_dirty();
    CHECK_FAILURE(delete_entry(header, parent, k_prime));
    return right.free();
}

bool BPTree::redistribute_nodes(Page& header, Page& parent, Page& left,
                                Page& right, int k_prime_index, int64_t k_prime)
{
    const TableID table_id = header.table_id();

    const int left_num_key = left.header().num_keys;
    const int right_num_key = right.header().num_keys;

    if (left_num_key < right_num_key)
    {
        if (left.header().is_leaf)
        {
            auto left_data = left.data();
            auto right_data = right.data();

            left_data[left_num_key] = right_data[0];
            parent.branches()[k_prime_index].key = right_data[1].key;

            for (int i = 0; i < right_num_key - 1; ++i)
                right_data[i] = right_data[i + 1];
        }
        else
        {
            auto left_branches = left.branches();
            auto right_branches = right.branches();

            left_branches[left_num_key].key = k_prime;
            const pagenum_t moved_child_pagenum =
                left_branches[left_num_key].child_page_number =
                    right.header().page_a_number;

            CHECK_FAILURE(buffer(
                [&](Page& tmp) {
                    tmp.header().parent_page_number = left.pagenum();

                    tmp.mark_dirty();

                    return true;
                },
                table_id, moved_child_pagenum));

            parent.branches()[k_prime_index].key = right_branches[0].key;

            right.header().page_a_number = right_branches[0].child_page_number;
            for (int i = 0; i < right_num_key - 1; ++i)
                right_branches[i] = right_branches[i + 1];
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
            parent.branches()[k_prime_index].key = right_data[0].key;
        }
        else
        {
            auto left_branches = left.branches();
            auto right_branches = right.branches();

            for (int i = right_num_key; i > 0; --i)
                right_branches[i] = right_branches[i - 1];

            right_branches[0].key = k_prime;
            const pagenum_t moved_child_pagenum =
                right_branches[0].child_page_number =
                    right.header().page_a_number;

            CHECK_FAILURE(buffer(
                [&](Page& tmp) {
                    tmp.header().parent_page_number = right.pagenum();

                    tmp.mark_dirty();

                    return true;
                },
                table_id, moved_child_pagenum));

            parent.branches()[k_prime_index].key =
                left_branches[left_num_key - 1].key;

            right.header().page_a_number =
                left_branches[left_num_key - 1].child_page_number;
        }

        --left.header().num_keys;
        ++right.header().num_keys;
    }

    left.mark_dirty();
    right.mark_dirty();
    parent.mark_dirty();

    return true;
}
