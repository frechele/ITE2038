#include "bpt.h"

#include "buffer.h"
#include "common.h"
#include "file.h"
#include "lock.h"
#include "log.h"

#include <cassert>

#include <algorithm>
#include <array>
#include <cstring>
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

int get_neighbor_index(const Page& parent, pagenum_t node)
{
    const pagenum_t pagenum = node;
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

bool BPTree::initialize(int num_buf)
{
    return BufferManager::initialize(num_buf);
}

bool BPTree::shutdown()
{
    return BufferManager::shutdown();
}

bool BPTree::open_table(Table& table)
{
    return BufMgr().open_table(table);
}

bool BPTree::close_table(Table& table)
{
    return BufMgr().close_table(table);
}

bool BPTree::insert(Table& table, const page_data_t& record)
{
    // case 1 : duplicated key
    if (find(table, record.key))
        return false;

    pagenum_t root_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& header) {
            root_page_number = header.header_page().root_page_number;
        },
        table));

    // case 2 : tree does not exist
    if (root_page_number == NULL_PAGE_NUM)
    {
        const auto new_node = make_node(table, true);
        CHECK_FAILURE(new_node != NULL_PAGE_NUM);

        CHECK_FAILURE(buffer(
            [&](Page& header) {
                header.header_page().root_page_number = new_node;

                header.mark_dirty();
            },
            table));

        return buffer(
            [&](Page& new_page) {
                new_page.header().parent_page_number = NULL_PAGE_NUM;
                new_page.header().num_keys = 1;

                new_page.data()[0] = record;

                new_page.mark_dirty();
            },
            table, new_node);
    }

    const pagenum_t leaf = find_leaf(table, record.key);

    int leaf_num_keys = 0;
    CHECK_FAILURE(
        buffer([&](Page& leaf) { leaf_num_keys = leaf.header().num_keys; },
               table, leaf));

    // case 3-1 : leaf has room for key
    if (leaf_num_keys < LEAF_ORDER - 1)
    {
        return insert_into_leaf(table, leaf, record);
    }

    return insert_into_leaf_after_splitting(table, leaf, record);
}

bool BPTree::remove(Table& table, int64_t key)
{
    auto record = find(table, key);
    CHECK_FAILURE(record.has_value());

    pagenum_t leaf = find_leaf(table, key);
    CHECK_FAILURE(leaf != NULL_PAGE_NUM);

    return delete_entry(table, leaf, key);
}

std::optional<page_data_t> BPTree::find(Table& table, int64_t key, Xact* xact)
{
    pagenum_t pid = find_leaf(table, key);
    CHECK_FAILURE2(pid != NULL_PAGE_NUM, std::nullopt);

    std::optional<page_data_t> result{ std::nullopt };

    Lock* lock_obj;
    bool need_wait = false;
    HierarchyID hid;
    CHECK_FAILURE2(
        buffer(
            [&](Page& page) {
                const int num_keys = page.header().num_keys;
                int i = binary_search_key(page.data(), num_keys, key);

                CHECK_FAILURE(i != num_keys);

                hid = HierarchyID(table.id(), pid, i);

                if (xact != nullptr)
                {
                    switch (xact->add_lock(hid, LockType::SHARED, &lock_obj))
                    {
                        case LockAcquireResult::DEADLOCK:
                        case LockAcquireResult::FAIL:
                            CHECK_FAILURE(XactMgr().abort(xact) && false);

                        case LockAcquireResult::NEED_TO_WAIT:
                            need_wait = true;
                            return true;

                        default:
                            break;
                    }
                }

                // not need to wait
                result = page.data()[i];

                return true;
            },
            table, pid),
        std::nullopt);

    if (need_wait)
    {
        lock_obj->wait();

        CHECK_FAILURE2(
            buffer([&](Page& page) { result = page.data()[hid.offset]; }, table,
                   pid),
            std::nullopt);
    }

    return result;
}

bool BPTree::update(Table& table, int64_t key, const char* value, Xact* xact)
{
    pagenum_t leaf = find_leaf(table, key);
    CHECK_FAILURE(leaf != NULL_PAGE_NUM);

    Lock* lock_obj;
    bool need_wait = false;
    HierarchyID hid;

    page_data_t old_data, new_data;
    new_data.key = key;
    strncpy(new_data.value, value, PAGE_DATA_VALUE_SIZE);

    CHECK_FAILURE(buffer(
        [&](Page& page) {
            const int num_keys = page.header().num_keys;
            auto data = page.data();

            int i = binary_search_key(data, num_keys, key);
            CHECK_FAILURE(i != num_keys);

            hid = HierarchyID(table.id(), leaf, i);
            old_data = data[i];

            if (xact != nullptr)
            {
                switch (xact->add_lock(hid, LockType::EXCLUSIVE, &lock_obj))
                {
                    case LockAcquireResult::DEADLOCK:
                    case LockAcquireResult::FAIL:
                        CHECK_FAILURE(XactMgr().abort(xact) && false);

                    case LockAcquireResult::NEED_TO_WAIT:
                        need_wait = true;
                        return true;

                    default:
                        break;
                }
            }

            // not need to wait
            LogMgr().log_update(xact, hid, PAGE_DATA_VALUE_SIZE - 8,
                                old_data, new_data);
            strncpy(page.data()[i].value, value, PAGE_DATA_VALUE_SIZE);
            page.mark_dirty();

            return true;
        },
        table, leaf));

    if (need_wait)
    {
        lock_obj->wait();

        CHECK_FAILURE(buffer(
            [&](Page& page) {
                LogMgr().log_update(xact, hid, PAGE_DATA_VALUE_SIZE - 8,
                                    old_data, new_data);
                strncpy(page.data()[hid.offset].value, value,
                        PAGE_DATA_VALUE_SIZE);

                page.mark_dirty();
            },
            table, leaf));
    }

    return true;
}

pagenum_t BPTree::make_node(Table& table, bool is_leaf)
{
    pagenum_t pagenum;
    CHECK_FAILURE2(BufMgr().create_page(table, is_leaf, pagenum),
                   NULL_PAGE_NUM);

    return pagenum;
}

pagenum_t BPTree::find_leaf(Table& table, int64_t key)
{
    pagenum_t root_page_number;
    CHECK_FAILURE2(buffer(
                       [&](Page& header) {
                           root_page_number =
                               header.header_page().root_page_number;
                       },
                       table),
                   NULL_PAGE_NUM);

    if (root_page_number == NULL_PAGE_NUM)
        return NULL_PAGE_NUM;

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
                        return;
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
                },
                table, current_num),
            NULL_PAGE_NUM);
    }

    return current_num;
}

int BPTree::path_to_root(Table& table, pagenum_t child_num)
{
    int length = 0;

    pagenum_t root_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& header) {
            root_page_number = header.header_page().root_page_number;
        },
        table));

    const pagenum_t root_page = root_page_number;
    while (child_num != root_page)
    {
        CHECK_FAILURE2(buffer(
                           [&](Page& child) {
                               child_num = child.header().parent_page_number;
                           },
                           table, child_num),
                       -1);

        ++length;
    }

    return length;
}

bool BPTree::insert_into_leaf(Table& table, pagenum_t leaf,
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
        },
        table, leaf);
}

bool BPTree::insert_into_parent(Table& table, pagenum_t left, pagenum_t right,
                                int64_t key)
{
    pagenum_t parent;
    CHECK_FAILURE(
        buffer([&](Page& left) { parent = left.header().parent_page_number; },
               table, left));

    // case 1 : new root
    if (parent == NULL_PAGE_NUM)
    {
        return insert_into_new_root(table, left, right, key);
    }

    // case 2 : leaf or node
    int num_keys, left_index;
    CHECK_FAILURE(buffer(
        [&](Page& parent) {
            num_keys = parent.header().num_keys;
            left_index = get_left_index(parent, left);
        },
        table, parent));

    // case 2-1 : the new key fits into the node
    if (num_keys < INTERNAL_ORDER - 1)
    {
        return insert_into_node(table, parent, left_index, right, key);
    }

    // case 2-2 : split a node in order to preserve the B+ tree
    // properties
    return insert_into_node_after_splitting(table, parent, left_index, right,
                                            key);
}

bool BPTree::insert_into_new_root(Table& table, pagenum_t left, pagenum_t right,
                                  int64_t key)
{
    pagenum_t new_root = make_node(table, false);
    CHECK_FAILURE(new_root != NULL_PAGE_NUM);

    CHECK_FAILURE(buffer(
        [&](Page& new_root) {
            new_root.header().num_keys = 1;
            new_root.header().page_a_number = left;

            new_root.branches()[0].key = key;
            new_root.branches()[0].child_page_number = right;

            new_root.mark_dirty();
        },
        table, new_root));

    CHECK_FAILURE(buffer(
        [&](Page& left) {
            left.header().parent_page_number = new_root;

            left.mark_dirty();
        },
        table, left));

    CHECK_FAILURE(buffer(
        [&](Page& right) {
            right.header().parent_page_number = new_root;

            right.mark_dirty();
        },
        table, right));

    return buffer(
        [&](Page& header) {
            header.header_page().root_page_number = new_root;

            header.mark_dirty();
        },
        table);
}

bool BPTree::insert_into_node(Table& table, pagenum_t parent, int left_index,
                              pagenum_t right, int64_t key)
{
    return buffer(
        [&](Page& parent) {
            const int num_keys = parent.header().num_keys;
            auto branches = parent.branches();

            for (int i = num_keys; i > left_index; --i)
            {
                branches[i] = branches[i - 1];
            }
            branches[left_index].child_page_number = right;
            branches[left_index].key = key;
            ++parent.header().num_keys;

            parent.mark_dirty();
        },
        table, parent);
}

bool BPTree::insert_into_leaf_after_splitting(Table& table, pagenum_t leaf,
                                              const page_data_t& record)
{
    std::array<page_data_t, LEAF_ORDER> temp_data;
    const int split_pivot = cut(LEAF_ORDER - 1);

    const pagenum_t new_leaf = make_node(table, true);
    CHECK_FAILURE(new_leaf != NULL_PAGE_NUM);

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

            leaf.header().page_a_number = new_leaf;

            leaf.mark_dirty();
        },
        table, leaf));

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
        },
        table, new_leaf));

    return insert_into_parent(table, leaf, new_leaf, insert_key);
}

bool BPTree::insert_into_node_after_splitting(Table& table, pagenum_t old,
                                              int left_index, pagenum_t right,
                                              int64_t key)
{
    std::array<page_branch_t, INTERNAL_ORDER> temp_data;
    const int split_pivot = cut(INTERNAL_ORDER);

    const pagenum_t new_page = make_node(table, false);
    CHECK_FAILURE(new_page != NULL_PAGE_NUM);

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
            temp_data[left_index].child_page_number = right;

            old.header().num_keys = 0;
            for (int i = 0; i < split_pivot - 1; ++i)
            {
                branches[i] = temp_data[i];
                ++old.header().num_keys;
            }

            parent_page_number = old.header().parent_page_number;

            old.mark_dirty();
        },
        table, old));

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
                    },
                    table, j));
            }

            new_page.mark_dirty();

            return true;
        },
        table, new_page));

    return insert_into_parent(table, old, new_page, k_prime);
}

bool BPTree::delete_entry(Table& table, pagenum_t node, int64_t key)
{
    pagenum_t parent;
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
            parent = node.header().parent_page_number;
        },
        table, node));

    pagenum_t root_page_number;
    CHECK_FAILURE(buffer(
        [&](Page& header) {
            root_page_number = header.header_page().root_page_number;
        },
        table));

    if (root_page_number == node)
        return adjust_root(table, node);

    if (node_num_keys > MERGE_THRESHOLD)
        return true;

    int neighbor_index, k_prime_index;
    int64_t k_prime;
    pagenum_t left;
    CHECK_FAILURE(buffer(
        [&](Page& parent) {
            neighbor_index = get_neighbor_index(parent, node);
            k_prime_index = (neighbor_index == -1) ? 0 : neighbor_index;
            k_prime = parent.branches()[k_prime_index].key;

            left = (neighbor_index == -1)
                       ? parent.branches()[0].child_page_number
                       : ((neighbor_index == 0)
                              ? parent.header().page_a_number
                              : parent.branches()[neighbor_index - 1]
                                    .child_page_number);
        },
        table, parent));

    const pagenum_t* left_ptr = &left;
    const pagenum_t* right_ptr = &node;

    if (neighbor_index == -1)
    {
        std::swap(left_ptr, right_ptr);
    }

    const int capacity = is_leaf ? LEAF_ORDER : INTERNAL_ORDER - 1;
    int left_num_keys;
    CHECK_FAILURE(
        buffer([&](Page& left) { left_num_keys = left.header().num_keys; },
               table, left));

    if (left_num_keys + node_num_keys < capacity)
        return coalesce_nodes(table, parent, *left_ptr, *right_ptr, k_prime);

    return redistribute_nodes(table, parent, *left_ptr, *right_ptr,
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

bool BPTree::adjust_root(Table& table, pagenum_t root)
{
    int num_keys, is_leaf;
    pagenum_t page_a_number;
    CHECK_FAILURE(buffer(
        [&](Page& root) {
            num_keys = root.header().num_keys;
            is_leaf = root.header().is_leaf;

            page_a_number = root.header().page_a_number;
        },
        table, root));

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
            },
            table, new_root_page_number));
    }

    CHECK_FAILURE(buffer(
        [&](Page& header) {
            header.header_page().root_page_number = new_root_page_number;

            header.mark_dirty();
        },
        table));

    return BufMgr().free_page(table, root);
}

bool BPTree::coalesce_nodes(Table& table, pagenum_t parent, pagenum_t left,
                            pagenum_t right, int64_t k_prime)
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
                                },
                                table, left_branches[i].child_page_number));

                            ++left.header().num_keys;
                            --right.header().num_keys;
                        }
                    }

                    left.mark_dirty();

                    return true;
                },
                table, right);
        },
        table, left));

    CHECK_FAILURE(delete_entry(table, parent, k_prime));

    return BufMgr().free_page(table, right);
}

bool BPTree::redistribute_nodes(Table& table, pagenum_t parent, pagenum_t left,
                                pagenum_t right, int k_prime_index,
                                int64_t k_prime)
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
                                return redistribute_nodes_to_left(
                                    table, parent, left, right, k_prime_index,
                                    k_prime);
                            }

                            return redistribute_nodes_to_right(
                                table, parent, left, right, k_prime_index,
                                k_prime);
                        },
                        table, right);
                },
                table, left);
        },
        table, parent);
}

bool BPTree::redistribute_nodes_to_left(Table& table, Page& parent, Page& left,
                                        Page& right, int k_prime_index,
                                        int64_t k_prime)
{
    const int left_num_key = left.header().num_keys;
    const int right_num_key = right.header().num_keys;

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
            },
            table, moved_child_pagenum));

        parent.branches()[k_prime_index].key = right_branches[0].key;

        right.header().page_a_number = right_branches[0].child_page_number;
        for (int i = 0; i < right_num_key - 1; ++i)
            right_branches[i] = right_branches[i + 1];
    }

    ++left.header().num_keys;
    --right.header().num_keys;

    left.mark_dirty();
    right.mark_dirty();
    parent.mark_dirty();

    return true;
}

bool BPTree::redistribute_nodes_to_right(Table& table, Page& parent, Page& left,
                                         Page& right, int k_prime_index,
                                         int64_t k_prime)
{
    const int left_num_key = left.header().num_keys;
    const int right_num_key = right.header().num_keys;

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
        right_branches[0].child_page_number = right.header().page_a_number;

        parent.branches()[k_prime_index].key =
            left_branches[left_num_key - 1].key;

        const pagenum_t moved_child_pagenum = right.header().page_a_number =
            left_branches[left_num_key - 1].child_page_number;

        CHECK_FAILURE(buffer(
            [&](Page& tmp) {
                tmp.header().parent_page_number = right.pagenum();

                tmp.mark_dirty();
            },
            table, moved_child_pagenum));
    }

    --left.header().num_keys;
    ++right.header().num_keys;

    left.mark_dirty();
    right.mark_dirty();
    parent.mark_dirty();

    return true;
}
