#include "buffer.h"

#include "common.h"
#include "page.h"

#include <cassert>
#include <memory.h>

void BufferBlock::lock()
{
    ++pin_count_;
}

void BufferBlock::unlock()
{
	assert(pin_count_ > 0);
    --pin_count_;
}

page_t& BufferBlock::frame()
{
	//assert(pin_count_ > 0);

	return *frame_;
}

void BufferBlock::mark_dirty() noexcept
{
	is_dirty_ = true;
}

void BufferBlock::clear()
{
	memset(frame_, 0, PAGE_SIZE);

	table_id_ = TableID();
	pagenum_ = NULL_PAGE_NUM;

	is_dirty_ = false;
	pin_count_ = 0;
}

BufferManager& BufferManager::get()
{
    static BufferManager instance;

    return instance;
}

bool BufferManager::initialize(int num_buf)
{
    if (num_buf <= 0)
	{
        return true;
	}

	page_arr_ = new (std::nothrow) page_t[num_buf];
	CHECK_FAILURE(page_arr_ != nullptr);

	for (int i = 0; i < num_buf; ++i)
	{
		BufferBlock* block = new (std::nothrow) BufferBlock;
		CHECK_FAILURE(block != nullptr);

		block->frame_ = &page_arr_[i];

		enqueue(block);
	}

    return true;
}

bool BufferManager::shutdown()
{
	// list is empty
	if (head_ == nullptr)
	{
		return true;
	}

	BufferBlock* tmp;
	BufferBlock* current = head_;
	do
	{
		if (current->is_dirty_)
		{
			const TableID table_id = current->table_id_;
			const pagenum_t pagenum = current->pagenum_;

			CHECK_FAILURE(TableManager::get(table_id).file_write_page(pagenum, current->frame_));
		}

		tmp = current->next_;
		delete current;
		current = tmp;
	} while (current != head_);

	delete[] page_arr_;

    return true;
}

bool BufferManager::close_table(int table_id)
{
	for (const auto& pr : block_tbl_)
	{
		auto [pr_table_id, _] = pr.first;

		if (pr_table_id == table_id)
		{
			// TODO: waiting pin == 0 with std::condition_variable
			CHECK_FAILURE(eviction(pr.second));
		}
	}

	return true;
}

std::optional<Page> BufferManager::get_page(TableID table_id, pagenum_t pagenum)
{
	auto it = block_tbl_.find({ table_id, pagenum });
	if (it != end(block_tbl_))
	{
		unlink_and_enqueue(it->second);
		return { Page(*it->second) };
	}

	BufferBlock* current = head_;
	if (current->table_id_ != -1)
		current = eviction();

	CHECK_FAILURE2(current != nullptr, std::nullopt);

	current->table_id_ = table_id;
	current->pagenum_ = pagenum;

	CHECK_FAILURE2(TableManager::get(table_id).file_read_page(pagenum, current->frame_), std::nullopt);
	block_tbl_.insert_or_assign({ table_id, pagenum }, current);

	unlink_and_enqueue(current);
	return { Page(*current) };
}

void BufferManager::enqueue(BufferBlock* block)
{
	// <- past       new ->
	//  head <> ... <> tail

	assert((head_ == nullptr && tail_ == nullptr) || (head_ != nullptr && tail_ != nullptr));

	// when linked list is empty
	if (head_ == nullptr)
	{
		head_ = block;
		tail_ = block;

		head_->next_ = block;
		tail_->prev_ = block;
	}
	else
	{
		block->prev_ = tail_;
		tail_->next_ = block;
		tail_ = block;
	}

	head_->prev_ = tail_;
	tail_->next_ = head_;
}

void BufferManager::unlink_and_enqueue(BufferBlock* block)
{
	BufferBlock* prev = block->prev_;
	BufferBlock* next = block->next_;

	block->prev_ = nullptr;
	block->next_ = nullptr;

	prev->next_ = next;
	next->prev_ = prev;

	if (block == head_)
	{
		head_ = next;
	}
	if (block == tail_)
	{
		tail_ = prev;
	}

	enqueue(block);
}

BufferBlock* BufferManager::eviction()
{
	BufferBlock* victim = head_;
	while (victim->pin_count_ > 0)
		victim = victim->next_;

	return eviction(victim);
}

BufferBlock* BufferManager::eviction(BufferBlock* block)
{
	const TableID table_id = block->table_id();
	const pagenum_t pagenum = block->pagenum();

	if (block->is_dirty_)
	{
		CHECK_FAILURE2(TableManager::get(table_id).file_write_page(pagenum, block->frame_), nullptr);
	}

	block->clear();
	block_tbl_.erase({ table_id, pagenum });

	return block;
}
