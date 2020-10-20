#include "page.h"

#include "file.h"

#include <memory.h>
#include <utility>

Page::Page(int table_id) noexcept : table_id_(table_id)
{
}

Page::Page(int table_id, pagenum_t pagenum) noexcept
    : table_id_(table_id), pagenum_(pagenum)
{
}

bool Page::load()
{
    return TableManager::get(table_id_).file_read_page(pagenum_, &impl_);
}

bool Page::commit()
{
    return TableManager::get(table_id_).file_write_page(pagenum_, &impl_);
}

bool Page::free()
{
    return TableManager::get(table_id_).file_free_page(pagenum_);
}

void Page::clear()
{
    memset(&impl_, 0, PAGE_SIZE);
}

pagenum_t Page::pagenum() const noexcept
{
    return pagenum_;
}

int Page::table_id() const noexcept
{
    return table_id_;
}

page_header_t& Page::header()
{
    return const_cast<page_header_t&>(std::as_const(*this).header());
}

const page_header_t& Page::header() const
{
    return impl_.node.header;
}

header_page_t& Page::header_page()
{
    return const_cast<header_page_t&>(std::as_const(*this).header_page());
}

const header_page_t& Page::header_page() const
{
    return impl_.file;
}

free_page_header_t& Page::free_header()
{
    return const_cast<free_page_header_t&>(std::as_const(*this).free_header());
}

const free_page_header_t& Page::free_header() const
{
    return impl_.node.free_header;
}

page_branch_t* Page::branches()
{
    return const_cast<page_branch_t*>(std::as_const(*this).branches());
}

const page_branch_t* Page::branches() const
{
    return impl_.node.branch;
}

page_data_t* Page::data()
{
    return const_cast<page_data_t*>(std::as_const(*this).data());
}

const page_data_t* Page::data() const
{
    return impl_.node.data;
}
