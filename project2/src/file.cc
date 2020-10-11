#include "file.h"

#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <memory.h>

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

    if ((file_handle_ = ::open(filename.c_str(),
        O_RDWR | O_CREAT | O_DSYNC ,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) == -1)
        return false;

    header_ = new header_page_t;
    if (!read(PAGE_SIZE, 0, header_))
    {
        memset(header_, 0, PAGE_SIZE);
        header_->num_pages = 1;

        update_header();
    }

    return true;
}

void FileManager::close()
{
    if (!is_open())
        return;

	delete header_;
    ::close(file_handle_);
}

bool FileManager::is_open() const
{
    return file_handle_ > 0;
}

header_page_t* FileManager::header() const
{
    return header_;
}

bool FileManager::update_header()
{
    return write(PAGE_SIZE, 0, header_);
}

bool FileManager::file_alloc_page(pagenum_t& pagenum)
{
    pagenum = NULL_PAGE_NUM;
    if (header_->free_page_number != NULL_PAGE_NUM)
    {
        page_t free_page;
        if (!file_read_page(header_->free_page_number, &free_page))
            return false;

        pagenum = header_->free_page_number;

        header_->free_page_number = free_page.header.next_free_page_id;
    }
    else
    {
        pagenum = header_->num_pages;

        page_t new_page;
        memset(&new_page, 0, PAGE_SIZE);

        if (!file_write_page(pagenum, &new_page))
            return false;

        ++header_->num_pages;
    }
    
    update_header();
    return true;
}

bool FileManager::file_free_page(pagenum_t pagenum)
{
    page_t new_page;
    memset(&new_page, 0, PAGE_SIZE);

    if (header_->free_page_number == NULL_PAGE_NUM)
    {
        header_->free_page_number = pagenum;
        if (!update_header())
            return false;
    }
    else
    {
        page_t last_free_page;
        if (!file_read_page(header_->free_page_number, &last_free_page))
            return false;

        last_free_page.header.next_free_page_id = header_->num_pages;
        if (!file_write_page(header_->free_page_number, &last_free_page))
            return false;
    }
    
    return file_write_page(pagenum, &new_page);
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
    return pread(file_handle_, value, size, offset) > 0;
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
    FileManager::get().file_alloc_page(alloced_page_num);

    return alloced_page_num;
}

void file_free_page(pagenum_t pagenum)
{
    FileManager::get().file_free_page(pagenum);
}

void file_read_page(pagenum_t pagenum, page_t* dest)
{
    FileManager::get().file_read_page(pagenum, dest);
}

void file_write_page(pagenum_t pagenum, const page_t* src)
{
    FileManager::get().file_write_page(pagenum, src);
}
