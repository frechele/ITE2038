#ifndef PAGE_H_
#define PAGE_H_

#include "file.h"

class BufferBlock;

class Page final
{
 public:
    Page(BufferBlock& block);
    ~Page() noexcept;

    void clear();
    void mark_dirty();

    [[nodiscard]] bool free(Page& header);

    [[nodiscard]] pagenum_t pagenum() const;
    [[nodiscard]] table_id_t table_id() const;

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

#endif  // PAGE_H_
