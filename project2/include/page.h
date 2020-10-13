#ifndef PAGE_H_
#define PAGE_H_

#include "common.h"

#include <cstdint>
#include <memory>

// SIZE CONSTANTS
constexpr size_t PAGE_DATA_VALUE_SIZE = 120;

constexpr size_t PAGE_HEADER_SIZE = 128;
constexpr size_t PAGE_HEADER_USED = 24;
constexpr size_t PAGE_HEADER_RESERVED = PAGE_HEADER_SIZE - PAGE_HEADER_USED;
constexpr size_t FREE_PAGE_HEADER_USED = 8;
constexpr size_t FREE_PAGE_HEADER_RESERVED =
    PAGE_HEADER_SIZE - FREE_PAGE_HEADER_USED;

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_DATA_SIZE = 128;
constexpr size_t PAGE_BRANCH_SIZE = 16;
constexpr size_t PAGE_DATA_IN_PAGE =
    (PAGE_SIZE - PAGE_HEADER_SIZE) / PAGE_DATA_SIZE;
constexpr size_t PAGE_BRANCHES_IN_PAGE =
    (PAGE_SIZE - PAGE_HEADER_SIZE) / PAGE_BRANCH_SIZE;

constexpr size_t HEADER_PAGE_USED = 24;
constexpr size_t HEADER_PAGE_RESERVED = PAGE_SIZE - HEADER_PAGE_USED;

// TYPES
using pagenum_t = uint64_t;
constexpr pagenum_t NULL_PAGE_NUM = 0;

struct page_data_t final
{
    int64_t key;
    char value[PAGE_DATA_VALUE_SIZE];
};

struct page_branch_t final
{
    int64_t key;
    pagenum_t child_page_number;
};

struct page_header_t final
{
    pagenum_t parent_page_number;

    int is_leaf;
    int num_keys;

    char reserved[PAGE_HEADER_RESERVED];

    pagenum_t page_a_number;
};

struct free_page_header_t final
{
    pagenum_t next_free_page_number;

    char reserved[FREE_PAGE_HEADER_RESERVED];
};

struct header_page_t final
{
    uint64_t free_page_number;
    uint64_t root_page_number;
    uint64_t num_pages;

    char reserved[HEADER_PAGE_RESERVED];
};

union page_t
{
    header_page_t file;

    struct
    {
        union
        {
            page_header_t header;
            free_page_header_t free_header;
        };

        union
        {
            page_data_t data[PAGE_DATA_IN_PAGE];
            page_branch_t branch[PAGE_BRANCHES_IN_PAGE];
        };
    } node;
};
using page_ptr_t = std::unique_ptr<page_t>;

class Page final
{
 public:
    Page() noexcept = default;
    explicit Page(pagenum_t pagenum) noexcept;

    [[nodiscard]] bool load();
    [[nodiscard]] bool commit();

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
