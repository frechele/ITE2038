#ifndef FILE_H_
#define FILE_H_

#include <set>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

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

class File final
{
 public:
    ~File();

    File() = default;
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    
    File(File&& other);
    File& operator=(File&& other);  

    [[nodiscard]] bool is_open() const;

    [[nodiscard]] bool file_alloc_page(pagenum_t& pagenum);
    [[nodiscard]] bool file_free_page(pagenum_t pagenum);
    [[nodiscard]] bool file_read_page(pagenum_t pagenum, page_t* dest);
    [[nodiscard]] bool file_write_page(pagenum_t pagenum, const page_t* src);

 private:
    [[nodiscard]] bool open(const std::string& filename);
    void close();

    [[nodiscard]] bool read(size_t size, size_t offset, void* value);
    [[nodiscard]] bool write(size_t size, size_t offset, const void* value);

 private:
    int file_handle_{ -1 };

    friend class TableManager;
};

class TableManager final
{
 public:
    static constexpr size_t MAX_TABLE_COUNT = 10;

 public:
    static TableManager& get();
    static File& get(int table_id);

    [[nodiscard]] std::optional<int> open_table(const std::string& filename);
    [[nodiscard]] bool close_table(int table_id);
    [[nodiscard]] bool is_open(int table_id) const;

 private:
    TableManager() = default;

 private:
    std::set<int> table_indicies_;
    std::unordered_map<int, File> tables_;
};

extern "C" {
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int table_id);

// Free an on-disk page to the free page list
void file_free_page(int table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);
}

#endif  // FILE_H_
