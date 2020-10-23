#ifndef PAGE_H_
#define PAGE_H_

#include "buffer.h"
#include "file.h"

class Page final
{
 public:
    Page(BufferBlock& block);

    void clear();
    void mark_dirty();

    void lock();
    void unlock();

    [[nodiscard]] bool free();

    [[nodiscard]] pagenum_t pagenum() const;
    [[nodiscard]] TableID table_id() const;

    [[nodiscard]] header_page_t& header_page();
    [[nodiscard]] const header_page_t& header_page() const;
    [[nodiscard]] page_header_t& header();
    [[nodiscard]] const page_header_t& header() const;
    [[nodiscard]] free_page_header_t& free_header();
    [[nodiscard]] const free_page_header_t& free_header() const;

    [[nodiscard]] page_branch_t* branches();
    [[nodiscard]] const page_branch_t* branches() const;
    [[nodiscard]] page_data_t* data();
    [[nodiscard]] const page_data_t* data() const;

 private:
    BufferBlock& block_;
};

class ScopedPageLock final
{
 public:
    ScopedPageLock(Page& page);
    ~ScopedPageLock();

 private:
    Page& page_;
};

#endif  // PAGE_H_
