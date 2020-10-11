#include "bpt.h"

#include <array>
#include <string.h>
#include <sstream>
#include <queue>

namespace
{
constexpr int cut(int length)
{
    return (length % 2) ? (length/2 + 1) : (length/2);
}

int get_left_index(const page_t& parent, pagenum_t left_num)
{
    if (parent.header.page_a_number == left_num)
        return 0;

    int left_index = 0;
    while (left_index < parent.header.num_keys &&
        parent.branch[left_index].child_page_id != left_num)
        ++left_index;

    return left_index + 1;
}
}

BPTree& BPTree::get()
{
    static BPTree instance;
    return instance;
}

bool BPTree::open(const std::string& filename)
{
    if (!FileManager::get().open(filename))
        return false;

    root_page_ = FileManager::get().header()->root_page_number;
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

    // case 2 : tree does not exist
    if (root_page_ == NULL_PAGE_NUM)
    {
        page_ptr_t new_page;
        std::tie(root_page_, new_page) = make_node(true);
        
        new_page->header.next_free_page_id = NULL_PAGE_NUM;
        new_page->header.num_keys = 1;

        new_page->data[0] = record;

        file_write_page(root_page_, new_page.get());

        FileManager::get().header()->root_page_number = root_page_;
        FileManager::get().update_header();

        return true;
    }

    auto leaf_node = find_leaf(record.key);
    auto& [leaf_num, leaf] = leaf_node;

    // case 3-1 : leaf has room for key
    if (leaf->header.num_keys < LEAF_ORDER - 1)
    {
        insert_into_leaf(leaf_node, record);
        return true;
    }

    // case 3-2 : leaf must be split
    
    insert_into_leaf_after_splitting(leaf_node, record);
    return true;
}

bool BPTree::remove(int64_t key)
{
    return false;
}

std::optional<page_data_t> BPTree::find(int64_t key) const
{
    const auto& [num, page] = find_leaf(key);
    if (page == nullptr)
        return std::nullopt;

    int i = 0;
    for (; i < page->header.num_keys; ++i)
        if (page->data[i].key == key)
            break;

    if (i == page->header.num_keys)
        return std::nullopt;

    return page->data[i];
}

std::vector<page_data_t> BPTree::find_range(int64_t key_start, int64_t key_end) const
{
    auto [pagenum, page] = find_leaf(key_start);

    if (pagenum == NULL_PAGE_NUM)
        return {};

    int i = 0;
    for (; i < page->header.num_keys && page->data[i].key < key_start; ++i);

    if (i == page->header.num_keys)
        return {};

    std::vector<page_data_t> result;
    while (pagenum != NULL_PAGE_NUM)
    {
        for (; i < page->header.num_keys && page->data[i].key <= key_end; ++i)
            result.push_back(page->data[i]);

        pagenum = page->header.page_a_number;
        file_read_page(pagenum, page.get());
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
            const pagenum_t page_num = queue.front();
            queue.pop();

            page_t page;
            file_read_page(page_num, &page);

            if (page.header.next_free_page_id != NULL_PAGE_NUM)
            {
                page_t parent;
                file_read_page(page.header.next_free_page_id, &parent);

                if (parent.header.page_a_number == page_num) // leftmost
                {
                    const int new_rank = path_to_root(page_num);
                    if (new_rank != rank)
                    {
                        rank = new_rank;
                        ss << '\n';
                    }
                }
            }

            for (int i = 0; i < page.header.num_keys; ++i)
                ss << (page.header.is_leaf ? page.data[i].key : page.branch[i].key) << ' ';

            if (!page.header.is_leaf && page.header.page_a_number != NULL_PAGE_NUM)
            {
                queue.emplace(page.header.page_a_number);
                for (int i = 0; i < page.header.num_keys; ++i)
                    queue.emplace(page.branch[i].child_page_id);
            }

            ss << "| ";
        }
    }

    return ss.str();
}

BPTree::node_t BPTree::make_node(bool is_leaf) const
{
    pagenum_t new_page_num = file_alloc_page();
    page_ptr_t new_page = std::make_unique<page_t>();
    
    new_page->header.is_leaf = is_leaf;

    file_write_page(new_page_num, new_page.get());
    return { new_page_num, std::move(new_page) };
}

BPTree::node_t BPTree::find_leaf(int64_t key) const
{
	if (root_page_ == NULL_PAGE_NUM)
		return { NULL_PAGE_NUM, nullptr };

	page_ptr_t current = std::make_unique<page_t>();
	pagenum_t page_num = root_page_;
	file_read_page(page_num, current.get());

	while (!current->header.is_leaf)
	{
		int child_idx = 0;
		while (child_idx < current->header.num_keys && key >= current->branch[child_idx].key)
			++child_idx;

        --child_idx;
		page_num = (child_idx == -1) ? current->header.page_a_number : current->branch[child_idx].child_page_id;
		file_read_page(page_num, current.get());
	}

	return { page_num, std::move(current) };
}

int BPTree::path_to_root(pagenum_t child_num) const
{
    int length = 0;

    while (child_num != root_page_)
    {
        page_t child;
        file_read_page(child_num, &child);

        child_num = child.header.next_free_page_id;
        ++length;
    }

    return length;
}

void BPTree::insert_into_leaf(node_t& leaf_node, const page_data_t& record)
{
    auto& [leaf_num, leaf] = leaf_node;

    int insertion_point = 0;
    while (insertion_point < leaf->header.num_keys && leaf->data[insertion_point].key < record.key)
        ++insertion_point;

    for (int i = leaf->header.num_keys; i > insertion_point; --i)
        leaf->data[i] = leaf->data[i - 1];

    leaf->data[insertion_point] = record;
    ++leaf->header.num_keys;

    file_write_page(leaf_num, leaf.get());
}

void BPTree::insert_into_parent(node_t& left_node, node_t& right_node, int64_t key)
{
    auto& [left_num, left] = left_node;
    auto& [right_num, right] = right_node;

    // case 1 : new root
    if (left->header.next_free_page_id == NULL_PAGE_NUM)
    {
        insert_into_new_root(left_node, right_node, key);
        return;
    }

    // case 2 : leaf or node
    node_t parent_node { left->header.next_free_page_id, std::make_unique<page_t>() };
    auto& [parent_num, parent] = parent_node;
    file_read_page(parent_num, parent.get());

    const int left_index = get_left_index(*parent, left_num);

    // case 2-1 : the new key fits into the node
    if (parent->header.num_keys < INTERNAL_ORDER - 1)
    {
        insert_into_node(parent_node, left_index, right_num, key);
        return;
    }

    // case 2-2 : split a node in order to preserve the B+ tree properties
    insert_into_node_after_splitting(parent_node, left_index, right_num, key);
}

void BPTree::insert_into_new_root(node_t& left_node, node_t& right_node, int64_t key)
{
    auto& [left_num, left] = left_node;
    auto& [right_num, right] = right_node;

    auto [new_root_num, new_root] = make_node(false);

    new_root->header.num_keys = 1;
    new_root->header.page_a_number = left_num;
    new_root->branch[0].key = key;
    new_root->branch[0].child_page_id = right_num;

    left->header.next_free_page_id = new_root_num;
    right->header.next_free_page_id = new_root_num;

    file_write_page(new_root_num, new_root.get());
    file_write_page(left_num, left.get());
    file_write_page(right_num, right.get());

    FileManager::get().header()->root_page_number = new_root_num;
    FileManager::get().update_header();

    root_page_ = new_root_num;
}

void BPTree::insert_into_node(node_t& parent_node, int left_index, pagenum_t right_num, int64_t key)
{
    auto& [parent_num, parent] = parent_node;

    for (int i = parent->header.num_keys; i > left_index; --i)
    {
        parent->branch[i] = parent->branch[i - 1];
    }
    parent->branch[left_index].child_page_id = right_num;
    parent->branch[left_index].key = key;
    ++parent->header.num_keys;

    file_write_page(parent_num, parent.get());
}

void BPTree::insert_into_leaf_after_splitting(node_t& leaf_node, const page_data_t& record)
{
    auto& [leaf_num, leaf] = leaf_node;

    auto new_leaf_node = make_node(true);
    auto& [new_leaf_num, new_leaf] = new_leaf_node;

    int insertion_point = 0;
    while (insertion_point < LEAF_ORDER - 1 && leaf->data[insertion_point].key < record.key)
        ++insertion_point;

    std::array<page_data_t, LEAF_ORDER> temp_data;
    for (int i = 0, j = 0; i < leaf->header.num_keys; ++i, ++j)
    {
        if (j == insertion_point) ++j;
        temp_data[j] = leaf->data[i];
    }

    temp_data[insertion_point] = record;

    leaf->header.num_keys = 0;

    const int split_pivot = cut(LEAF_ORDER - 1);
    for (int i = 0; i < split_pivot; ++i)
    {
        leaf->data[i] = temp_data[i];
        ++leaf->header.num_keys;
    }

    for (int i = split_pivot, j = 0; i < LEAF_ORDER; ++i, ++j)
    {
        new_leaf->data[j] = temp_data[i];
        ++new_leaf->header.num_keys;
    }

    new_leaf->header.page_a_number = leaf->header.page_a_number;
    leaf->header.page_a_number = new_leaf_num;

    // update parent
    new_leaf->header.next_free_page_id = leaf->header.next_free_page_id;

    file_write_page(new_leaf_num, new_leaf.get());
    file_write_page(leaf_num, leaf.get());
    insert_into_parent(leaf_node, new_leaf_node, new_leaf->data[0].key);
}

void BPTree::insert_into_node_after_splitting(node_t& old_node, int left_index, pagenum_t right_num, int64_t key)
{
    auto& [old_num, old] = old_node;

    std::array<page_branch_t, INTERNAL_ORDER> temp_data;

    for (int i = 0, j = 0; i < old->header.num_keys; ++i, ++j)
    {
        if (j == left_index) ++j;
        temp_data[j] = old->branch[i];
    }
    temp_data[left_index].key = key;
    temp_data[left_index].child_page_id = right_num;

    const int split_pivot = cut(INTERNAL_ORDER);

    old->header.num_keys = 0;
    for (int i = 0; i < split_pivot - 1; ++i)
    {
        old->branch[i] = temp_data[i];
        ++old->header.num_keys;
    }

    auto new_node = make_node(false);
    auto& [new_num, new_page] = new_node;

    const int64_t k_prime = temp_data[split_pivot - 1].key;
    new_page->header.page_a_number = temp_data[split_pivot - 1].child_page_id;
    for (int i = split_pivot, j = 0; i < INTERNAL_ORDER; ++i, ++j)
    {
        new_page->branch[j] = temp_data[i];
        ++new_page->header.num_keys;
    }
    new_page->header.next_free_page_id = old->header.next_free_page_id;

    for (int i = -1; i < new_page->header.num_keys; ++i)
    {
        const pagenum_t j = (i == -1) ? new_page->header.page_a_number : new_page->branch[i].child_page_id;

        page_t child;
        file_read_page(j, &child);

        child.header.next_free_page_id = new_num;

        file_write_page(j, &child);
    }

    file_write_page(old_num, old.get());
    file_write_page(new_num, new_page.get());

    insert_into_parent(old_node, new_node, k_prime);
}
