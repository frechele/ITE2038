#include "file.h"

#include "common.h"
#include "page.h"

#include <fcntl.h>
#include <memory.h>
#include <unistd.h>

FileManager& FileManager::get()
{
    static FileManager instance;
    return instance;
}

FileManager::~FileManager()
{
    close();
}

bool FileManager::open(const std::string& filename)
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

void FileManager::close()
{
    if (!is_open())
        return;

    ::close(file_handle_);
}

bool FileManager::is_open() const
{
    return file_handle_ > 0;
}

bool FileManager::file_alloc_page(pagenum_t& pagenum)
{
    pagenum = NULL_PAGE_NUM;

    Page header;

    CHECK_FAILURE(header.load());
    if (header.header_page().free_page_number != NULL_PAGE_NUM)
    {
        Page free_page(header.header_page().free_page_number);

        CHECK_FAILURE(free_page.load());

        pagenum = header.header_page().free_page_number;

        header.header_page().free_page_number = free_page.free_header().next_free_page_number;
    }
    else
    {
        pagenum = header.header_page().num_pages;

        Page new_page(pagenum);
        new_page.clear();

        CHECK_FAILURE(new_page.commit());

        ++header.header_page().num_pages;
    }

    return header.commit();
}

bool FileManager::file_free_page(pagenum_t pagenum)
{
    Page header;

    CHECK_FAILURE(header.load());

    Page free_page(pagenum);
    free_page.free_header().next_free_page_number = header.header_page().free_page_number;

    header.header_page().free_page_number = pagenum;
    
    CHECK_FAILURE(header.commit());
    return free_page.commit();
}

bool FileManager::file_read_page(pagenum_t pagenum, page_t* dest)
{
    return read(PAGE_SIZE, pagenum * PAGE_SIZE, dest);
}

bool FileManager::file_write_page(pagenum_t pagenum, const page_t* src)
{
    return write(PAGE_SIZE, pagenum * PAGE_SIZE, src);
}

bool FileManager::read(size_t size, size_t offset, void* value)
{
    return pread(file_handle_, value, size, offset) >= 0;
}

bool FileManager::write(size_t size, size_t offset, const void* value)
{
    if (pwrite(file_handle_, value, size, offset) == -1)
        return false;

    fsync(file_handle_);
    return true;
}

pagenum_t file_alloc_page()
{
    pagenum_t alloced_page_num = NULL_PAGE_NUM;

    if (!FileManager::get().file_alloc_page(alloced_page_num))
        exit(EXIT_FAILURE);

    return alloced_page_num;
}

void file_free_page(pagenum_t pagenum)
{
    if (!FileManager::get().file_free_page(pagenum))
        exit(EXIT_FAILURE);
}

void file_read_page(pagenum_t pagenum, page_t* dest)
{
    if (!FileManager::get().file_read_page(pagenum, dest))
        exit(EXIT_FAILURE);
}

void file_write_page(pagenum_t pagenum, const page_t* src)
{
    if (!FileManager::get().file_write_page(pagenum, src))
        exit(EXIT_FAILURE);
}
