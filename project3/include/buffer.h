#ifndef BUFFER_H_
#define BUFFER_H_

#include "file.h"

#include <map>
#include <memory>
#include <optional>

class Page;

class BufferBlock final
{
 public:
    void lock();
    void unlock();

    [[nodiscard]] page_t& frame();

    [[nodiscard]] constexpr TableID table_id() noexcept
    {
        return table_id_;
    }
    [[nodiscard]] constexpr pagenum_t pagenum() noexcept
    {
        return pagenum_;
    }

    void mark_dirty() noexcept;

 private:
    void clear();

 private:
    page_t* frame_;
    TableID table_id_;
    pagenum_t pagenum_{ NULL_PAGE_NUM };

    bool is_dirty_{ false };
    int pin_count_{ 0 };

    BufferBlock* prev_{ nullptr };
    BufferBlock* next_{ nullptr };

    friend class BufferManager;
};

class BufferManager final
{
 public:
    static BufferManager& get();

    [[nodiscard]] bool initialize(int num_buf);
    [[nodiscard]] bool shutdown();

    [[nodiscard]] bool close_table(int table_id);

    [[nodiscard]] std::optional<Page> get_page(
        TableID table_id, pagenum_t pagenum = NULL_PAGE_NUM);

 private:
    BufferManager() = default;

    void enqueue(BufferBlock* block);
    void unlink_and_enqueue(BufferBlock* block);

    [[nodiscard]] BufferBlock* eviction();
    [[nodiscard]] BufferBlock* eviction(BufferBlock* block);

 private:
    BufferBlock* head_{ nullptr };
    BufferBlock* tail_{ nullptr };

    page_t* page_arr_{ nullptr };

    std::map<table_page_t, BufferBlock*> block_tbl_;
};

#endif  // BUFFER_H_
