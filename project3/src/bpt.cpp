#include "bpt.h"

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
    auto header = BufferManager::get().get_page(table_id);
    CHECK_FAILURE(header.has_value());

    ScopedPageLock header_lock(header.value());

    // case 1 : duplicated key
    if (find(header.value(), record.key))
        return false;

    // case 2 : tree does not exist
    if (header.value().header_page().root_page_number == NULL_PAGE_NUM)
    {
        auto new_page = make_node(header.value(), true);
        CHECK_FAILURE(new_page.has_value());

        ScopedPageLock new_page_lock(new_page.value());

        new_page.value().header().parent_page_number = NULL_PAGE_NUM;
        new_page.value().header().num_keys = 1;

        new_page.value().data()[0] = record;

        header.value().header_page().root_page_number = new_page.value().pagenum();

        new_page.value().mark_dirty();
        header.value().mark_dirty();

        return true;
    }

    auto leaf = find_leaf(header.value(), record.key);
    ScopedPageLock leaf_Lock(leaf.value());

    // case 3-1 : leaf has room for key
    if (leaf.value().header().num_keys < LEAF_ORDER - 1)
    {
        return insert_into_leaf(header.value(), leaf.value(), record);
    }

    // case 3-2 : leaf must be split

    return insert_into_leaf_after_splitting(header.value(), leaf.value(), record);
}

bool BPTree::remove(TableID table_id, int64_t key)
{
    auto header = BufferManager::get().get_page(table_id);
    CHECK_FAILURE(header.has_value());

    ScopedPageLock header_lock(header.value());

    auto record = find(header.value(), key);
    CHECK_FAILURE(record);

    auto leaf = find_leaf(header.value(), key);
    CHECK_FAILURE(leaf.has_value());

    ScopedPageLock leaf_lock(leaf.value());

    return delete_entry(header.value(), std::move(leaf.value()), key);
}

std::optional<page_data_t> BPTree::find(TableID table_id, int64_t key) const
{
    auto header = BufferManager::get().get_page(table_id);
    CHECK_FAILURE2(header.has_value(), std::nullopt);

    ScopedPageLock header_lock(header.value());

    return find(header.value(), key);
}

std::vector<page_data_t> BPTree::find_range(TableID table_id, int64_t key_start,
                                            int64_t key_end) const
{
    auto header = BufferManager::get().get_page(table_id);
    CHECK_FAILURE2(header.has_value(), {});

    ScopedPageLock header_lock(header.value());

    auto page = find_leaf(header.value(), key_start);
    CHECK_FAILURE2(page.has_value(), {});

    page.value().lock();

    const int num_keys = page.value().header().num_keys;
    const auto data = page.value().data();

    int i = 0;
    for (; i < num_keys && data[i].key < key_start; ++i)
        ;

    if (i == num_keys)
        return {};

    std::vector<page_data_t> result;
    while (page.value().pagenum() != NULL_PAGE_NUM)
    {
        const int num_keys = page.value().header().num_keys;
        const auto data = page.value().data();

        for (; i < num_keys && data[i].key <= key_end; ++i)
            result.push_back(data[i]);

        page.value().unlock();
        page.emplace(BufferManager::get().get_page(table_id, page.value().header().page_a_number).value());
        page.value().unlock();
        CHECK_FAILURE2(page.has_value(), {});
        i = 0;
    }

    page.value().unlock();

    return result;
}

std::string BPTree::to_string(TableID table_id) const
{
    std::stringstream ss;

    auto header = BufferManager::get().get_page(table_id);
    CHECK_FAILURE2(header.has_value(), "");

    ScopedPageLock header_lock(header.value());

    if (header.value().header_page().root_page_number != NULL_PAGE_NUM)
    {
        int rank = 0;
        std::queue<pagenum_t> queue;
        queue.emplace(header.value().header_page().root_page_number);

        while (!queue.empty())
        {
            const pagenum_t pagenum = queue.front();
            queue.pop();

            auto page = BufferManager::get().get_page(table_id, pagenum);
            CHECK_FAILURE2(page.has_value(), "");

            ScopedPageLock page_lock(page.value());

            if (page.value().header().page_a_number != NULL_PAGE_NUM)
            {
                auto parent = BufferManager::get().get_page(table_id, page.value().header().parent_page_number);
                CHECK_FAILURE2(parent.has_value(), "");
                ScopedPageLock parent_lock(parent.value());

                if (parent.value().header().page_a_number == pagenum)  // leftmost
                {
                    const int new_rank = path_to_root(header.value(), pagenum);
                    if (new_rank != rank)
                    {
                        rank = new_rank;
                        ss << '\n';
                    }
                }
            }

            const int num_keys = page.value().header().num_keys;
            const int is_leaf = page.value().header().is_leaf;
            const pagenum_t page_a_number = page.value().header().page_a_number;
            const auto data = page.value().data();
            const auto branches = page.value().branches();

            for (int i = 0; i < num_keys; ++i)
                ss << (is_leaf ? data[i].key : branches[i].key) << ' ';

            if (!is_leaf && page_a_number != NULL_PAGE_NUM)
            {
                queue.emplace(page_a_number);
                for (int i = 0; i < num_keys; ++i)
                    queue.emplace(branches[i].child_page_number);
            }

            ss << "| ";
        }
    }

    return ss.str();
}

std::optional<Page> BPTree::make_node(Page& header, bool is_leaf) const
{
    const TableID table_id = header.table_id();

    pagenum_t pagenum;
    CHECK_FAILURE2(TableManager::get(table_id).file_alloc_page(pagenum), std::nullopt);

    auto page = BufferManager::get().get_page(table_id, pagenum);
    CHECK_FAILURE2(page.has_value(), std::nullopt);

    ScopedPageLock page_lock(page.value());

    page.value().clear();
    page.value().header().is_leaf = is_leaf;

    page.value().mark_dirty();

    return page;
}

std::optional<page_data_t> BPTree::find(Page& header, int64_t key) const
{
    auto page = find_leaf(header, key);
    CHECK_FAILURE2(page.has_value(), std::nullopt);

    const int num_keys = page.value().header().num_keys;
    int i = binary_search_key(page.value().data(), num_keys, key);

    CHECK_FAILURE2(i != num_keys, std::nullopt);

    return page.value().data()[i];
}

std::optional<Page> BPTree::find_leaf(Page& header, int64_t key) const
{
    if (header.header_page().root_page_number == NULL_PAGE_NUM)
        return std::nullopt;

    const TableID table_id = header.table_id();
    auto current = BufferManager::get().get_page(table_id, header.header_page().root_page_number);
    CHECK_FAILURE2(current.has_value(), std::nullopt);

    current.value().lock();

    while (!current.value().header().is_leaf)
    {
        const auto branches = current.value().branches();

        int child_idx = std::distance(
            branches,
            std::upper_bound(
                branches, branches + current.value().header().num_keys, key,
                [](auto lhs, const auto& rhs) { return lhs < rhs.key; }));

        --child_idx;

        current.value().unlock();
        current.emplace(BufferManager::get().get_page(table_id, (child_idx == -1)
                                     ? current.value().header().page_a_number
                                     : branches[child_idx].child_page_number).value());
        CHECK_FAILURE2(current.has_value(), std::nullopt);
        current.value().lock();
    }

    current.value().unlock();

    return current;
}

int BPTree::path_to_root(Page& header, pagenum_t child_num) const
{
    int length = 0;

    const TableID table_id = header.table_id();
    const pagenum_t root_page = header.pagenum();
    while (child_num != root_page)
    {
        auto child = BufferManager::get().get_page(table_id, child_num);
        CHECK_FAILURE2(child.has_value(), -1);

        child_num = child.value().header().parent_page_number;
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
    auto parent = BufferManager::get().get_page(header.table_id(), left.header().parent_page_number);
    CHECK_FAILURE(parent.has_value());

    const int left_index = get_left_index(parent.value(), left.pagenum());

    // case 2-1 : the new key fits into the node
    if (parent.value().header().num_keys < INTERNAL_ORDER - 1)
    {
        return insert_into_node(header, parent.value(), left_index, right, key);
    }

    // case 2-2 : split a node in order to preserve the B+ tree properties
    return insert_into_node_after_splitting(header, parent.value(), left_index, right,
                                            key);
}

bool BPTree::insert_into_new_root(Page& header, Page& left, Page& right,
                                  int64_t key)
{
    auto new_root = make_node(header, false);
    CHECK_FAILURE(new_root.has_value());

    new_root.value().header().num_keys = 1;
    new_root.value().header().page_a_number = left.pagenum();
    new_root.value().branches()[0].key = key;
    new_root.value().branches()[0].child_page_number = right.pagenum();

    left.header().parent_page_number = new_root.value().pagenum();
    right.header().parent_page_number = new_root.value().pagenum();

    new_root.value().mark_dirty();
    left.mark_dirty();
    right.mark_dirty();

    header.header_page().root_page_number = new_root.value().pagenum();

    header.mark_dirty();

    return true;
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
    auto new_leaf = make_node(header, true);

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

    auto new_data = new_leaf.value().data();

    for (int i = split_pivot, j = 0; i < LEAF_ORDER; ++i, ++j)
    {
        new_data[j] = temp_data[i];
        ++new_leaf.value().header().num_keys;
    }

    new_leaf.value().header().page_a_number = leaf.header().page_a_number;
    leaf.header().page_a_number = new_leaf.value().pagenum();

    new_leaf.value().header().parent_page_number = leaf.header().parent_page_number;

    new_leaf.value().mark_dirty();
    leaf.mark_dirty();

    return insert_into_parent(header, leaf, new_leaf.value(), new_data[0].key);
}

bool BPTree::insert_into_node_after_splitting(Page& header, Page& old, int left_index,
                                              Page& right, int64_t key)
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

    auto new_page = make_node(header, false);

    auto new_branches = new_page.value().branches();

    const int64_t k_prime = temp_data[split_pivot - 1].key;
    new_page.value().header().page_a_number =
        temp_data[split_pivot - 1].child_page_number;
    for (int i = split_pivot, j = 0; i < INTERNAL_ORDER; ++i, ++j)
    {
        new_branches[j] = temp_data[i];
        ++new_page.value().header().num_keys;
    }
    new_page.value().header().parent_page_number = old.header().parent_page_number;

    const int new_keys = new_page.value().header().num_keys;
    for (int i = -1; i < new_keys; ++i)
    {
        const pagenum_t j = (i == -1) ? new_page.value().header().page_a_number
                                      : new_branches[i].child_page_number;

        auto child = BufferManager::get().get_page(table_id, j);

        CHECK_FAILURE(child.has_value());

        child.value().header().parent_page_number = new_page.value().pagenum();

        child.value().mark_dirty();
    }

    old.mark_dirty();
    new_page.value().mark_dirty();

    return insert_into_parent(header, old, new_page.value(), k_prime);
}

bool BPTree::delete_entry(Page& header, Page node, int64_t key)
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

    auto parent = BufferManager::get().get_page(table_id, node.header().parent_page_number);
    CHECK_FAILURE(parent.has_value());

    const int neighbor_index = get_neighbor_index(parent.value(), node);
    const int k_prime_index = (neighbor_index == -1) ? 0 : neighbor_index;
    const int64_t k_prime = parent.value().branches()[k_prime_index].key;

    const int left_pagenum =
        (neighbor_index == -1)
            ? parent.value().branches()[0].child_page_number
            : ((neighbor_index == 0)
                   ? parent.value().header().page_a_number
                   : parent.value().branches()[neighbor_index - 1].child_page_number);
    auto left = BufferManager::get().get_page(table_id, left_pagenum);
    CHECK_FAILURE(left.has_value());

    Page* left_ptr = &left.value();
    Page* right_ptr = &node;

    if (neighbor_index == -1)
    {
        std::swap(left_ptr, right_ptr);
    }

    const int capacity = is_leaf ? LEAF_ORDER : INTERNAL_ORDER - 1;
    if (left.value().header().num_keys + node.header().num_keys < capacity)
        return coalesce_nodes(header, parent.value(), *left_ptr, *right_ptr, k_prime);

    return redistribute_nodes(header, parent.value(), *left_ptr, *right_ptr, k_prime_index, k_prime);
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

        auto new_root = BufferManager::get().get_page(header.table_id(), header.header_page().root_page_number);
        CHECK_FAILURE(new_root.has_value());

        new_root.value().header().parent_page_number = 0;

        new_root.value().mark_dirty();
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

            auto tmp = BufferManager::get().get_page(table_id, left_branches[i].child_page_number);
            CHECK_FAILURE(tmp.has_value());

            tmp.value().header().parent_page_number = left_number;

            tmp.value().mark_dirty();

            ++left.header().num_keys;
            --right.header().num_keys;
        }
    }

    left.mark_dirty();
    CHECK_FAILURE(delete_entry(header, parent, k_prime));
    return right.free();
}

bool BPTree::redistribute_nodes(Page& header, Page& parent, Page& left, Page& right,
                                int k_prime_index, int64_t k_prime)
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

            auto tmp = BufferManager::get().get_page(table_id, moved_child_pagenum);
            CHECK_FAILURE(tmp.has_value());

            tmp.value().header().parent_page_number = left.pagenum();

            tmp.value().mark_dirty();

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

            auto tmp = BufferManager::get().get_page(table_id, moved_child_pagenum);
            CHECK_FAILURE(tmp.has_value());

            tmp.value().header().parent_page_number = right.pagenum();

            tmp.value().mark_dirty();

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
