#include "bpt.h"

#include "file.h"

#include <string.h>
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
}  // namespace

BPTree& BPTree::get()
{
    static BPTree instance;
    return instance;
}

bool BPTree::open(const std::string& filename)
{
    CHECK_FAILURE(FileManager::get().open(filename));

    Page header;

    CHECK_FAILURE(header.load());

    root_page_ = header.header_page().root_page_number;

    return true;
}

bool BPTree::is_open() const
{
    return FileManager::get().is_open();
}

bool BPTree::insert(const page_data_t& record)
{
    // case 1 : duplicated key
    if (find(record.key))
        return false;

    Page header;

    // case 2 : tree does not exist
    if (root_page_ == NULL_PAGE_NUM)
    {
        Page new_page = make_node(true);

        new_page.header().parent_page_number = NULL_PAGE_NUM;
        new_page.header().num_keys = 1;

        new_page.data()[0] = record;

        CHECK_FAILURE(new_page.commit());

        Page header;

        CHECK_FAILURE(header.load());

        root_page_ = new_page.pagenum();
        header.header_page().root_page_number = root_page_;

        return header.commit();
    }

    Page leaf = find_leaf(record.key);
    CHECK_FAILURE(leaf.load());

    // case 3-1 : leaf has room for key
    if (leaf.header().num_keys < LEAF_ORDER - 1)
    {
        return insert_into_leaf(leaf, record);
    }

    // case 3-2 : leaf must be split

    return insert_into_leaf_after_splitting(leaf, record);
}

bool BPTree::remove(int64_t key)
{
    return false;
}

std::optional<page_data_t> BPTree::find(int64_t key) const
{
    Page page = find_leaf(key);
    if (page.pagenum() == NULL_PAGE_NUM)
        return std::nullopt;

    const int num_keys = page.header().num_keys;
    int i = 0;
    for (; i < num_keys; ++i)
        if (page.data()[i].key == key)
            break;

    if (i == num_keys)
        return std::nullopt;

    return page.data()[i];
}

std::vector<page_data_t> BPTree::find_range(int64_t key_start,
                                            int64_t key_end) const
{
    Page page = find_leaf(key_start);

    if (page.pagenum() == NULL_PAGE_NUM)
        return {};

    CHECK_FAILURE2(page.load(), {});

    const int num_keys = page.header().num_keys;
    const auto data = page.data();

    int i = 0;
    for (; i < num_keys && data[i].key < key_start; ++i)
        ;

    if (i == num_keys)
        return {};

    std::vector<page_data_t> result;
    while (page.pagenum() != NULL_PAGE_NUM)
    {
        const int num_keys = page.header().num_keys;
        const auto data = page.data();

        for (; i < num_keys && data[i].key <= key_end; ++i)
            result.push_back(data[i]);

        page = Page(page.header().page_a_number);
        CHECK_FAILURE2(page.load(), {});
        i = 0;
    }

    return result;
}

std::string BPTree::to_string() const
{
    std::stringstream ss;

    if (root_page_ != NULL_PAGE_NUM)
    {
        int rank = 0;
        std::queue<pagenum_t> queue;
        queue.emplace(root_page_);

        while (!queue.empty())
        {
            const pagenum_t pagenum = queue.front();
            queue.pop();

            Page page(pagenum);
            CHECK_FAILURE2(page.load(), "");

            if (page.header().page_a_number != NULL_PAGE_NUM)
            {
                Page parent(page.header().parent_page_number);
                CHECK_FAILURE2(parent.load(), "");

                if (parent.header().page_a_number == pagenum)  // leftmost
                {
                    const int new_rank = path_to_root(pagenum);
                    if (new_rank != rank)
                    {
                        rank = new_rank;
                        ss << '\n';
                    }
                }
            }

            const int num_keys = page.header().num_keys;
            const int is_leaf = page.header().is_leaf;
            const pagenum_t page_a_number = page.header().page_a_number;
            const auto data = page.data();
            const auto branches = page.branches();

            for (int i = 0; i < num_keys; ++i)
                ss << (is_leaf ? data[i].key : branches[i].key) << ' ';

            if (!is_leaf &&
                page_a_number != NULL_PAGE_NUM)
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

Page BPTree::make_node(bool is_leaf) const
{
    Page page(file_alloc_page());

    CHECK_FAILURE2(page.load(), Page());

    page.header().is_leaf = is_leaf;

    CHECK_FAILURE2(page.commit(), Page());

    return page;
}

Page BPTree::find_leaf(int64_t key) const
{
    if (root_page_ == NULL_PAGE_NUM)
        return Page();

    Page current(root_page_);
    CHECK_FAILURE2(current.load(), Page());

    while (!current.header().is_leaf)
    {
        const auto branches = current.branches();

        int child_idx = std::distance(
            branches,
            std::upper_bound(
                branches, branches + current.header().num_keys,
                key, [](auto lhs, const auto& rhs) { return lhs < rhs.key; }));

        --child_idx;
        current = Page((child_idx == -1)
                       ? current.header().page_a_number
                       : branches[child_idx].child_page_number);
        CHECK_FAILURE2(current.load(), Page());
    }

    return current;
}

int BPTree::path_to_root(pagenum_t child_num) const
{
    int length = 0;

    while (child_num != root_page_)
    {
        Page child(child_num);
        CHECK_FAILURE2(child.load(), -1);

        child_num = child.header().parent_page_number;
        ++length;
    }

    return length;
}

bool BPTree::insert_into_leaf(Page& leaf, const page_data_t& record)
{
    const int num_keys = leaf.header().num_keys;
    auto data = leaf.data();

    int insertion_point = std::distance(
        data,
        std::lower_bound(
            data, data + num_keys, record.key,
            [](const auto& lhs, auto rhs) { return lhs.key < rhs; }));

    for (int i = num_keys; i > insertion_point; --i)
        data[i] = data[i - 1];

    data[insertion_point] = record;
    ++leaf.header().num_keys;

    return leaf.commit();
}

bool BPTree::insert_into_parent(Page& left, Page& right,
                                int64_t key)
{
    // case 1 : new root
    if (left.header().parent_page_number == NULL_PAGE_NUM)
    {
        return insert_into_new_root(left, right, key);
    }

    // case 2 : leaf or node
    Page parent(left.header().parent_page_number);
    CHECK_FAILURE(parent.load());

    const int left_index = get_left_index(parent, left.pagenum());

    // case 2-1 : the new key fits into the node
    if (parent.header().num_keys < INTERNAL_ORDER - 1)
    {
        return insert_into_node(parent, left_index, right, key);
    }

    // case 2-2 : split a node in order to preserve the B+ tree properties
    return insert_into_node_after_splitting(parent, left_index, right, key);
}

bool BPTree::insert_into_new_root(Page& left, Page& right,
                                  int64_t key)
{
    Page new_root = make_node(false);

    new_root.header().num_keys = 1;
    new_root.header().page_a_number = left.pagenum();
    new_root.branches()[0].key = key;
    new_root.branches()[0].child_page_number = right.pagenum();

    left.header().parent_page_number = new_root.pagenum();
    right.header().parent_page_number = new_root.pagenum();

    CHECK_FAILURE(new_root.commit());
    CHECK_FAILURE(left.commit());
    CHECK_FAILURE(right.commit());

    Page header;

    CHECK_FAILURE(header.load());

    header.header_page().root_page_number = new_root.pagenum();

    CHECK_FAILURE(header.commit());

    root_page_ = new_root.pagenum();

    return true;
}

bool BPTree::insert_into_node(Page& parent, int left_index,
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

    return parent.commit();
}

bool BPTree::insert_into_leaf_after_splitting(Page& leaf,
                                              const page_data_t& record)
{
    Page new_leaf = make_node(true);

    const int num_keys = leaf.header().num_keys;
    const auto data = leaf.data();

    int insertion_point = std::distance(
        data, std::lower_bound(data, data + (LEAF_ORDER - 1),
                                     record.key, [](const auto& lhs, auto rhs) {
                                         return lhs.key < rhs;
                                     }));

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

    auto new_data = new_leaf.data();

    for (int i = split_pivot, j = 0; i < LEAF_ORDER; ++i, ++j)
    {
        new_data[j] = temp_data[i];
        ++new_leaf.header().num_keys;
    }

    new_leaf.header().page_a_number = leaf.header().page_a_number;
    leaf.header().page_a_number = new_leaf.pagenum();

    // update parent
    new_leaf.header().parent_page_number = leaf.header().parent_page_number;

    CHECK_FAILURE(new_leaf.commit());
    CHECK_FAILURE(leaf.commit());
    return insert_into_parent(leaf, new_leaf, new_data[0].key);
}

bool BPTree::insert_into_node_after_splitting(Page& old, int left_index,
                                              Page& right, int64_t key)
{
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

    Page new_page = make_node(false);

    auto new_branches = new_page.branches();

    const int64_t k_prime = temp_data[split_pivot - 1].key;
    new_page.header().page_a_number =
        temp_data[split_pivot - 1].child_page_number;
    for (int i = split_pivot, j = 0; i < INTERNAL_ORDER; ++i, ++j)
    {
        new_branches[j] = temp_data[i];
        ++new_page.header().num_keys;
    }
    new_page.header().parent_page_number = old.header().parent_page_number;

    const int new_keys = new_page.header().num_keys;
    for (int i = -1; i < new_keys; ++i)
    {
        const pagenum_t j = (i == -1) ? new_page.header().page_a_number
                                      : new_branches[i].child_page_number;

        Page child(j);

        CHECK_FAILURE(child.load());

        child.header().parent_page_number = new_page.pagenum();

        CHECK_FAILURE(child.commit());
    }

    CHECK_FAILURE(old.commit());
    CHECK_FAILURE(new_page.commit());

    return insert_into_parent(old, new_page, k_prime);
}
