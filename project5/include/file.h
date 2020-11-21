#ifndef FILE_H_
#define FILE_H_

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "types.h"

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

class Table;
class Page;

class File final
{
 public:
    static constexpr uint64_t NEW_PAGES_WHEN_NO_FREE_PAGES = 1;

 public:
    ~File();

    File() = default;
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    File(File&& other);
    File& operator=(File&& other);

    [[nodiscard]] const std::string& filename() const;

    [[nodiscard]] bool is_open() const;

    [[nodiscard]] bool file_alloc_page(Page& header, pagenum_t& pagenum);

    [[nodiscard]] bool file_read_page(pagenum_t pagenum, page_t* dest);
    [[nodiscard]] bool file_write_page(pagenum_t pagenum, const page_t* src);

 private:
    [[nodiscard]] bool open(const std::string& filename);
    void close();

    [[nodiscard]] bool extend(Page& header, uint64_t new_pages);
    [[nodiscard]] size_t capacity() const;

    [[nodiscard]] bool read(size_t size, size_t offset, void* value);
    [[nodiscard]] bool write(size_t size, size_t offset, const void* value);

 private:
    std::string filename_;
    int file_handle_{ -1 };

    friend class FileManager;
};

class FileManager final
{
public:
    [[nodiscard]] static bool initialize();
    [[nodiscard]] static bool shutdown();

    [[nodiscard]] static FileManager& get_instnace();

    [[nodiscard]] bool open_table(Table& table);
    [[nodiscard]] bool close_table(Table& table);

private:
    std::unordered_map<std::string, File> files_;

    inline static FileManager* instance_{ nullptr };
};

inline FileManager& FileMgr()
{
    return FileManager::get_instnace();
}

#endif  // FILE_H_
