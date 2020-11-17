#ifndef BUFFER_H_
#define BUFFER_H_

#include "common.h"
#include "file.h"
#include "page.h"
#include "table.h"

#include <memory>
#include <optional>
#include <type_traits>
#include <unordered_map>

class BufferBlock final
{
 public:
    void lock();
    void unlock();

    [[nodiscard]] page_t& frame();

    [[nodiscard]] constexpr table_id_t table_id() noexcept
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
    table_id_t table_id_{ -1 };
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
    [[nodiscard]] static bool initialize(int num_buf);
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static BufferManager& get_instance();

    [[nodiscard]] bool open_table(Table& table);
    [[nodiscard]] bool close_table(Table& table);

    [[nodiscard]] bool create_page(Table& table, bool is_leaf,
                                   pagenum_t& pagenum);
    [[nodiscard]] bool free_page(Table& table, pagenum_t pagenum);

    [[nodiscard]] bool get_page(Table& table, pagenum_t pagenum,
                                std::optional<Page>& page);

    bool check_all_unpinned() const;
    void dump_frame_stat() const;

 private:
    BufferManager() = default;
    [[nodiscard]] bool init_lru(int num_buf);
    [[nodiscard]] bool shutdown_lru();

    void enqueue(BufferBlock* block);
    void unlink_and_enqueue(BufferBlock* block);

    [[nodiscard]] BufferBlock* eviction();
    [[nodiscard]] BufferBlock* eviction(BufferBlock* block);

    [[nodiscard]] bool clear_block(BufferBlock* block);

 private:
    BufferBlock* head_{ nullptr };
    BufferBlock* tail_{ nullptr };

    page_t* page_arr_{ nullptr };

    std::unordered_map<table_id_t, std::unordered_map<pagenum_t, BufferBlock*>>
        block_tbl_;

    inline static BufferManager* instance_{ nullptr };
};

inline BufferManager& BufMgr()
{
    return BufferManager::get_instance();
}

template <typename Function>
[[nodiscard]] bool buffer(Function&& func, Table& table,
                          pagenum_t pagenum = NULL_PAGE_NUM)
{
    std::optional<Page> opt;
    CHECK_FAILURE(BufMgr().get_page(table, pagenum, opt));

    if constexpr (std::is_void<decltype(func(opt.value()))>::value)
    {
        func(opt.value());
        return true;
    }
    else
    {
        return func(opt.value());
    }
}
#endif  // BUFFER_H_
