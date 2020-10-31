#include "buffer.h"

#include "dbms.h"
#include "page.h"

#include <memory.h>
#include <cassert>

#include <iostream>

void BufferBlock::lock()
{
    assert(pin_count_ < 1);
    ++pin_count_;
}

void BufferBlock::unlock()
{
    assert(pin_count_ > 0);
    --pin_count_;
}

page_t& BufferBlock::frame()
{
    assert(pin_count_ > 0);
    return *frame_;
}

void BufferBlock::mark_dirty() noexcept
{
    is_dirty_ = true;
}

void BufferBlock::clear()
{
    memset(frame_, 0, PAGE_SIZE);

    table_id_ = -1;
    pagenum_ = NULL_PAGE_NUM;

    is_dirty_ = false;
    pin_count_ = 0;
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

    block_tbl_.reserve(num_buf);

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
        while (current->pin_count_ > 0)
            ;

        if (current->is_dirty_)
        {
            const table_id_t table_id = current->table_id_;
            const pagenum_t pagenum = current->pagenum_;

            CHECK_FAILURE(TblMgr().get(table_id).file_write_page(
                pagenum, current->frame_));
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
    auto& tbl_map = block_tbl_[table_id];

    for (const auto& pr : tbl_map)
    {
        while (pr.second->pin_count_ > 0)
            ;

        CHECK_FAILURE(eviction(pr.second));
    }

    return TblMgr().close_table(table_id);
}

bool BufferManager::get_page(table_id_t table_id, pagenum_t pagenum,
                             std::optional<Page>& page)
{
    BufferBlock* current = head_;

    auto tblIt = block_tbl_.find(table_id);
    std::unordered_map<pagenum_t, BufferBlock*>::iterator it;
    if (tblIt != end(block_tbl_) && (it = tblIt->second.find(pagenum)) != tblIt->second.end())
    {
        current = it->second;
        current->lock();
    }
    else
    {
        if (current->table_id_ != -1)
            current = eviction();

        CHECK_FAILURE(current != nullptr);

        current->lock();

        current->table_id_ = table_id;
        current->pagenum_ = pagenum;

        CHECK_FAILURE(TblMgr().get(table_id).file_read_page(
            pagenum, current->frame_));

        if (tblIt == end(block_tbl_))
        {
            block_tbl_.insert_or_assign(table_id, std::unordered_map<pagenum_t, BufferBlock*>());
        }
        block_tbl_[table_id].insert_or_assign(pagenum, current);
    }

    unlink_and_enqueue(current);
    page.emplace(*current);

    return true;
}

void BufferManager::enqueue(BufferBlock* block)
{
    // <- past       new ->
    //  head <> ... <> tail

    assert((head_ == nullptr && tail_ == nullptr) ||
           (head_ != nullptr && tail_ != nullptr));

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
    while (victim->pin_count_ > 0) {
        victim = victim->next_;
        assert(victim != head_);
    }

    return eviction(victim);
}

BufferBlock* BufferManager::eviction(BufferBlock* block)
{
    // dump_frame_stat();

    const table_id_t table_id = block->table_id();
    const pagenum_t pagenum = block->pagenum();

    if (block->is_dirty_)
    {
        CHECK_FAILURE2(
            TblMgr().get(table_id).file_write_page(pagenum, block->frame_),
            nullptr);
    }

    block->clear();
    block_tbl_[table_id].erase(pagenum);

    return block;
}

bool BufferManager::check_all_unpinned() const
{
    BufferBlock* current = head_;

    do
    {
        if (current->pin_count_ > 0)
            return false;
    } while (current != head_);

    return true;
}

void BufferManager::dump_frame_stat() const
{
    BufferBlock* current = head_;

    int id = 0;
    do
    {
        std::cerr << "id: " << id << " tid: " << current->table_id_
                  << " pid: " << current->pagenum_
                  << " pin: " << current->pin_count_ << '\n';

        ++id;
        current = current->next_;
    } while (current != head_);
}
