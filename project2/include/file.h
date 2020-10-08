#ifndef __PAGE_H__
#define __PAGE_H__

#include <cstdint>
#include <memory>
#include <string>

// SIZE CONSTANTS
constexpr size_t PAGE_DATA_VALUE_SIZE = 120;

constexpr size_t PAGE_HEADER_SIZE = 128;
constexpr size_t PAGE_HEADER_USED = 24;
constexpr size_t PAGE_HEADER_RESERVED = PAGE_HEADER_SIZE - PAGE_HEADER_USED;

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_DATA_SIZE = 128;
constexpr size_t PAGE_BRANCH_SIZE = 16;
constexpr size_t PAGE_DATA_IN_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) / PAGE_DATA_SIZE;
constexpr size_t PAGE_BRANCHES_IN_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) / PAGE_BRANCH_SIZE;

constexpr size_t HEADER_PAGE_USED = 24;
constexpr size_t HEADER_PAGE_RESERVED = PAGE_SIZE - HEADER_PAGE_USED;

// TYPES
using pagenum_t = uint64_t;
constexpr pagenum_t NULL_PAGE_NUM = 0;

struct page_data_t {
    int64_t key;
    char value[PAGE_DATA_VALUE_SIZE];
};

struct page_branch_t {
    int64_t key;
    pagenum_t child_page_id;
};

struct page_header_t {
    pagenum_t next_free_page_id;

    int is_leaf;
    int num_keys;

    char reserved[PAGE_HEADER_RESERVED];

    pagenum_t page_a_number;
};

struct page_t {
    page_header_t header;

    union {
        page_data_t data[PAGE_DATA_IN_PAGE];
        page_branch_t branch[PAGE_BRANCHES_IN_PAGE];
    };
};
using page_ptr_t = std::unique_ptr<page_t>;

struct header_page_t {
    uint64_t free_page_number;
    uint64_t root_page_number;
    uint64_t num_pages;

    char reserved[HEADER_PAGE_RESERVED];
};

class FileManager final
{
public:
    static FileManager& get();

    ~FileManager();

    bool open(const std::string& filename);
    void close();

    bool is_open() const;

    bool read(size_t size, size_t offset, void* value);
    bool write(size_t size, size_t offset, const void* value);
    
    header_page_t* header() const;
    void update_header();

private:
    int file_handle_{ -1 };
    header_page_t* header_{ nullptr };
};

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);

#endif  // __PAGE_H__
