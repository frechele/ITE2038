#include "bpt.h"

#include <array>
#include <string.h>

namespace
{
constexpr int cut(int length)
{
    return (length % 2) ? (length/2 + 1) : (length/2);
}
}

BPTree& BPTree::get()
{
    static BPTree instance;
    return instance;
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

        memcpy(&new_page->data[0], &record, PAGE_DATA_SIZE);

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

std::optional<page_data_t> BPTree::find(int64_t key)
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

BPTree::node_t BPTree::make_node(bool is_leaf) const
{
    pagenum_t new_page_num = file_alloc_page();
    page_ptr_t new_page = std::make_unique<page_t>();
    
    new_page->header.is_leaf = is_leaf;

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

		page_num = current->branch[child_idx].child_page_id;
		file_read_page(page_num, current.get());
	}

	return { page_num, std::move(current) };
}

void BPTree::insert_into_leaf(node_t& leaf_node, const page_data_t& record)
{
    auto& [leaf_num, leaf] = leaf_node;

    int insertion_point = 0;
    while (insertion_point < leaf->header.num_keys && leaf->data[insertion_point].key < record.key)
        ++insertion_point;

    for (int i = leaf->header.num_keys; i > insertion_point; --i)
        memcpy(&leaf->data[i], &leaf->data[i - 1], PAGE_DATA_SIZE);

    memcpy(&leaf->data[insertion_point], &record, PAGE_DATA_SIZE);
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


}

void BPTree::insert_into_new_root(node_t& left_node, node_t& right_node, int64_t key)
{

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
        memcpy(&temp_data[j], &leaf->data[i], PAGE_DATA_SIZE);
    }

    memcpy(&temp_data[insertion_point], &record, PAGE_DATA_SIZE);

    leaf->header.num_keys = 0;

    const int split_pivot = cut(LEAF_ORDER - 1);
    for (int i = 0; i < split_pivot; ++i)
    {
        memcpy(&leaf->data[i], &temp_data[i], PAGE_DATA_SIZE);
        ++leaf->header.num_keys;
    }

    for (int i = split_pivot, j = 0; i < LEAF_ORDER; ++i, ++j)
    {
        memcpy(&new_leaf->data[j], &temp_data[i], PAGE_DATA_SIZE);
        ++new_leaf->header.num_keys;
    }

    right_sibling_page_number(new_leaf->header) = right_sibling_page_number(leaf->header);
    right_sibling_page_number(leaf->header) = new_leaf_num;

    // update parent
    new_leaf->header.next_free_page_id = leaf->header.next_free_page_id;

    insert_into_parent(leaf_node, new_leaf_node, new_leaf->data[0].key);
}
