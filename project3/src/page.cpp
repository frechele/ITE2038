#include "page.h"

#include "file.h"

#include <memory.h>
#include <utility>

Page::Page(BufferBlock& block)
    : block_(block)
{
}

void Page::clear()
{
    memset(&block_.frame(), 0, PAGE_SIZE);
}

void Page::mark_dirty()
{
	block_.mark_dirty();
}

void Page::lock()
{
    block_.lock();
}

void Page::unlock()
{
    block_.unlock();
}

bool Page::free()
{
    return TableManager::get(table_id()).file_free_page(pagenum());
}

pagenum_t Page::pagenum() const 
{
    return block_.pagenum();
}

TableID Page::table_id() const 
{
    return block_.table_id();
}

page_header_t& Page::header()
{
    return const_cast<page_header_t&>(std::as_const(*this).header());
}

const page_header_t& Page::header() const
{
    return block_.frame().node.header;
}

header_page_t& Page::header_page()
{
    return const_cast<header_page_t&>(std::as_const(*this).header_page());
}

const header_page_t& Page::header_page() const
{
    return block_.frame().file;
}

free_page_header_t& Page::free_header()
{
    return const_cast<free_page_header_t&>(std::as_const(*this).free_header());
}

const free_page_header_t& Page::free_header() const
{
    return block_.frame().node.free_header;
}

page_branch_t* Page::branches()
{
    return const_cast<page_branch_t*>(std::as_const(*this).branches());
}

const page_branch_t* Page::branches() const
{
    return block_.frame().node.branch;
}

page_data_t* Page::data()
{
    return const_cast<page_data_t*>(std::as_const(*this).data());
}

const page_data_t* Page::data() const
{
    return block_.frame().node.data;
}

ScopedPageLock::ScopedPageLock(Page& page) : page_(page)
{
    page.lock();
}

ScopedPageLock::~ScopedPageLock()
{
    page_.unlock();
}
