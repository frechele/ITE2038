#include "file.h"

#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <memory.h>

FileManager& FileManager::get() {
    static FileManager instance;

    return instance;
}

FileManager::~FileManager() {
    close();
}

bool FileManager::open(const std::string& filename) {
    if (is_open())
        close();

    if ((file_handle_ = ::open(filename.c_str(),
        O_RDWR | O_APPEND | O_CREAT | O_DSYNC,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) == -1)
        return false;

    header_ = new header_page_t;
    if (!read(PAGE_SIZE, 0, header_)) {
        memset(header_, 0, PAGE_SIZE);
        update_header();
    }

    return true;
}

void FileManager::close() {
    if (!is_open())
        return;

	delete header_;
    ::close(file_handle_);
}

bool FileManager::is_open() const {
    return file_handle_ > 0;
}

bool FileManager::read(size_t size, size_t offset, void* value) {
    return pread(file_handle_, value, size, offset) > 0;
}

bool FileManager::write(size_t size, size_t offset, const void* value) {
    return pwrite(file_handle_, value, size, offset) != -1;
}

header_page_t* FileManager::header() const {
    return header_;
}

void FileManager::update_header() {
    write(PAGE_SIZE, 0, header_);
}

pagenum_t file_alloc_page() {
    if (!FileManager::get().is_open())
        exit(EXIT_FAILURE);

    auto header = FileManager::get().header();

    pagenum_t alloced_page_num = NULL_PAGE_NUM;
    if (header->free_page_number != NULL_PAGE_NUM)
    {
        page_t free_page;
        file_read_page(header->free_page_number, &free_page);

        header->free_page_number = free_page.header.next_free_page_id;

        alloced_page_num = header->free_page_number;
    }
    else
    {
        alloced_page_num = ++header->num_pages;
    
        page_t new_page;
        memset(&new_page, 0, PAGE_SIZE);

        file_write_page(alloced_page_num, &new_page);
    }

    FileManager().get().update_header();
    return alloced_page_num;
}

void file_free_page(pagenum_t pagenum) {
    auto header = FileManager().get().header();

    page_t new_page;
    memset(&new_page, 0, PAGE_SIZE);

    if (header->free_page_number == NULL_PAGE_NUM) {
        header->free_page_number = pagenum;
        FileManager::get().update_header();
    } else {
        page_t last_free_page;
        file_read_page(header->free_page_number, &last_free_page);

        last_free_page.header.next_free_page_id = header->num_pages;
        file_write_page(header->free_page_number, &last_free_page);
    }

    file_write_page(pagenum, &new_page);
}

void file_read_page(pagenum_t pagenum, page_t* dest) {
    FileManager::get().read(PAGE_SIZE, pagenum * PAGE_SIZE, dest);
}

void file_write_page(pagenum_t pagenum, const page_t* src) {
    FileManager::get().write(PAGE_SIZE, pagenum * PAGE_SIZE, src);
}
