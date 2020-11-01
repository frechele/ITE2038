#include "file.h"

#include "buffer.h"
#include "common.h"
#include "page.h"
#include "table.h"

#include <fcntl.h>
#include <memory.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>

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

    filename_ = filename;

    return true;
}

void File::close()
{
    if (!is_open())
        return;

    ::close(file_handle_);
}

const std::string& File::filename() const
{
    return filename_;
}

bool File::is_open() const
{
    return file_handle_ > 0;
}

bool File::file_alloc_page(Page& header, pagenum_t& pagenum)
{
    pagenum = NULL_PAGE_NUM;

    if (header.header_page().free_page_number != NULL_PAGE_NUM)
    {
        const pagenum_t free_page_number = header.header_page().free_page_number;

        pagenum = header.header_page().free_page_number;

        page_t free_page;
        CHECK_FAILURE(file_read_page(free_page_number, &free_page));

        header.header_page().free_page_number = free_page.node.free_header.next_free_page_number;

        CHECK_FAILURE(file_write_page(free_page_number, &free_page));
    }
    else
    {
        pagenum = header.header_page().num_pages;

        if (capacity() <= header.header_page().num_pages)
            CHECK_FAILURE(extend(header, NEW_PAGES_WHEN_NO_FREE_PAGES));

        ++header.header_page().num_pages;
    }
    
    header.mark_dirty();

    return true;
}

bool File::extend(Page& header, uint64_t new_pages)
{
    const uint64_t prev_size = header.header_page().num_pages;
    const uint64_t new_size = prev_size + new_pages;

    return ftruncate(file_handle_, new_size * PAGE_SIZE) == 0;
}

size_t File::capacity() const
{
    struct stat s;
    fstat(file_handle_, &s);

    assert(s.st_size % PAGE_SIZE == 0);

    return s.st_size / PAGE_SIZE;
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

bool FileManager::initialize()
{
    CHECK_FAILURE(instance_ == nullptr);

    instance_ = new (std::nothrow) FileManager;

    return instance_ != nullptr;
}

bool FileManager::shutdown()
{
    CHECK_FAILURE(instance_ != nullptr);

    delete instance_;
    instance_ = nullptr;

    return true;
}

FileManager& FileManager::get_instnace()
{
    return *instance_;
}

bool FileManager::open_table(Table& table)
{
    auto it = files_.find(table.filename());
    CHECK_FAILURE(it == end(files_));

    File file;
    CHECK_FAILURE(file.open(table.filename()));

    files_.insert_or_assign(table.filename(), std::move(file));

    table.set_file(&files_[table.filename()]);

    return true;
}

bool FileManager::close_table(Table& table)
{
    auto it = files_.find(table.filename());
    CHECK_FAILURE(it != end(files_));

    table.set_file(nullptr);

    it->second.close();
    files_.erase(it);

    return true;
}
