#include "file.h"

#include "common.h"
#include "page.h"

#include <fcntl.h>
#include <memory.h>
#include <unistd.h>

File::~File()
{
    close();
}

File::File(File&& other)
{
    file_handle_ = other.file_handle_;
    other.file_handle_ = -1;
}

File& File::operator=(File&& other)
{
    file_handle_ = other.file_handle_;
    other.file_handle_ = -1;

    return *this;
}

bool File::open(const std::string& filename)
{
    if (is_open())
        close();

    const bool create_new = (access(filename.c_str(), F_OK) == -1);

    if ((file_handle_ = ::open(
             filename.c_str(), O_RDWR | O_CREAT | O_DSYNC,
             S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) == -1)
        return false;

    if (create_new)
    {
        page_t file_header;
        memset(&file_header, 0, PAGE_SIZE);
        file_header.file.num_pages = 1;

        CHECK_FAILURE(file_write_page(0, &file_header));
    }

    return true;
}

void File::close()
{
    if (!is_open())
        return;

    ::close(file_handle_);
}

bool File::is_open() const
{
    return file_handle_ > 0;
}

bool File::file_alloc_page(pagenum_t& pagenum)
{
    pagenum = NULL_PAGE_NUM;

    page_t header;
    CHECK_FAILURE(file_read_page(0, &header));

    if (header.file.free_page_number != NULL_PAGE_NUM)
    {
        const pagenum_t free_page_number = header.file.free_page_number;
        page_t free_page;
        CHECK_FAILURE(file_read_page(free_page_number, &free_page));

        pagenum = header.file.free_page_number;

        header.file.free_page_number = free_page.node.free_header.next_free_page_number;
    }
    else
    {
        pagenum = header.file.num_pages;

        page_t new_page;
        memset(&new_page, 0, PAGE_SIZE);

        CHECK_FAILURE(file_write_page(pagenum, &new_page));

        ++header.file.num_pages;
    }

    return file_write_page(0, &header);
}

bool File::file_free_page(pagenum_t pagenum)
{
    page_t header;
    CHECK_FAILURE(file_read_page(0, &header));

    page_t free_page;
    free_page.node.free_header.next_free_page_number = header.file.free_page_number;

    header.file.free_page_number = pagenum;
    
    CHECK_FAILURE(file_write_page(0, &header));
    return file_write_page(pagenum, &free_page);
}

bool File::file_read_page(pagenum_t pagenum, page_t* dest)
{
    return read(PAGE_SIZE, pagenum * PAGE_SIZE, dest);
}

bool File::file_write_page(pagenum_t pagenum, const page_t* src)
{
    return write(PAGE_SIZE, pagenum * PAGE_SIZE, src);
}

bool File::read(size_t size, size_t offset, void* value)
{
    return pread(file_handle_, value, size, offset) >= 0;
}

bool File::write(size_t size, size_t offset, const void* value)
{
    if (pwrite(file_handle_, value, size, offset) == -1)
        return false;

    fsync(file_handle_);
    return true;
}

TableManager& TableManager::get()
{
    static TableManager instance;
    return instance;
}

File& TableManager::get(int table_id)
{
    return get().tables_[table_id];
}

std::optional<int> TableManager::open_table(const std::string& filename)
{
    CHECK_FAILURE2(tables_.size() < MAX_TABLE_COUNT, std::nullopt);

    int table_id = 1;
    for (int t : table_indicies_)
    {
        if (t > table_id)
            break;

        ++table_id;
    }

    File file;
    CHECK_FAILURE2(file.open(filename), std::nullopt);

    table_indicies_.emplace(table_id);
    tables_.insert_or_assign(table_id, std::move(file));
    return table_id;
}

bool TableManager::close_table(int table_id)
{
    auto it = tables_.find(table_id);
    
    if (it == end(tables_))
        return false;

    table_indicies_.erase(it->first);
    it->second.close();
    tables_.erase(it);

    return true;
}

bool TableManager::is_open(int table_id) const
{
    return tables_.find(table_id) != end(tables_);
}
