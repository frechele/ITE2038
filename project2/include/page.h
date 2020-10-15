#ifndef PAGE_H_
#define PAGE_H_

#include "common.h"
#include "file.h"

class Page final
{
 public:
    Page() noexcept = default;
    explicit Page(pagenum_t pagenum) noexcept;

    [[nodiscard]] bool load();
    [[nodiscard]] bool commit();
    [[nodiscard]] bool free();

    void clear();

    [[nodiscard]] pagenum_t pagenum() const noexcept;

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
    pagenum_t pagenum_{ NULL_PAGE_NUM };
    page_t impl_;
};

#endif  // PAGE_H_
