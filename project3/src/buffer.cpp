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
	return frame_;
}

void BufferBlock::mark_dirty() noexcept
{
	is_dirty_ = true;
}

void BufferBlock::clear()
{
	memset(&frame_, 0, PAGE_SIZE);

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

	try
	{
		frames_.resize(num_buf);
	}
	catch (...)
	{
		return false;
	}

    return true;
}

bool BufferManager::shutdown()
{
	for (auto& current : frames_)
	{
		if (current.is_dirty_)
		{
			const TableID table_id = current.table_id_;
			const pagenum_t pagenum = current.pagenum_;

			CHECK_FAILURE(TableManager::get(table_id).file_write_page(pagenum, &current.frame_));
		}
	}

	frames_.clear();

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
			CHECK_FAILURE(eviction(*pr.second));
		}
	}

	return true;
}

std::optional<Page> BufferManager::get_page(TableID table_id, pagenum_t pagenum)
{
	auto it = block_tbl_.find({ table_id, pagenum });
	if (it != end(block_tbl_))
		return { Page(*it->second) };

	if (frames_[clk_].table_id_ != -1)
		CHECK_FAILURE2(eviction(), std::nullopt);

	// now clk points empty frame
	auto& current = frames_[clk_];

	current.table_id_ = table_id;
	current.pagenum_ = pagenum;

	CHECK_FAILURE2(TableManager::get(table_id).file_read_page(pagenum, &current.frame_), std::nullopt);
	block_tbl_.insert_or_assign({ table_id, pagenum }, &current);

	return { Page(current) };
}

bool BufferManager::eviction()
{
	const size_t frame_cnt = frames_.size();

	// find un-pinned buffer block
	for (; frames_[clk_].pin_count_ > 0; clk_ = (clk_ + 1) % frame_cnt);

	return eviction(frames_[clk_]);
}

bool BufferManager::eviction(BufferBlock& frame)
{
	const TableID table_id = frame.table_id();
	const pagenum_t pagenum = frame.pagenum();

	if (frame.is_dirty_)
	{
		CHECK_FAILURE(TableManager::get(table_id).file_write_page(pagenum, &frame.frame()));
	}

	frame.clear();
	block_tbl_.erase({ table_id, pagenum });

	return true;
}
