#ifndef __PAGE_H__
#define __PAGE_H__

#include <stdint.h>

// SIZE CONSTANTS
#define PAGE_DATA_VALUE_SIZE 120

#define PAGE_HEADER_SIZE 128
#define PAGE_HEADER_USED 16
#define PAGE_HEADER_RESERVED (PAGE_HEADER_SIZE - PAGE_HEADER_USED)

#define PAGE_SIZE 4096
#define PAGE_DATA_IN_PAGE 31
#define PAGE_BRANCHES_IN_PAGE 248

#define HEADER_PAGE_USED 24
#define HEADER_PAGE_RESERVED (PAGE_SIZE - HEADER_PAGE_USED)

// TYPES
typedef uint64_t pagenum_t;

typedef struct page_data_t {
    int64_t key;
    char value[PAGE_DATA_VALUE_SIZE];
} page_data_t;

typedef struct page_branch_t {
    int64_t key;
    pagenum_t child_page_id;
} page_branch_t;

typedef struct page_header_t {
    pagenum_t next_free_page_id;

    int is_leaf;
    int num_keys;

    char reserved[PAGE_HEADER_RESERVED];
} page_header_t;

typedef struct page_t {
    page_header_t header;

    union {
        page_data_t data[PAGE_DATA_IN_PAGE];
        page_branch_t branch[PAGE_BRANCHES_IN_PAGE];
    };
} page_t;

typedef struct header_page_t {
    uint64_t free_page_number;
    uint64_t root_page_number;
    uint64_t num_pages;

    char reserved[HEADER_PAGE_RESERVED];
} header_page_t;

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);

#endif  // __PAGE_H__
