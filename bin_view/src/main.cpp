#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <iomanip>

#include "bpt.h"
#include "common.h"
#include "dbapi.h"

using std::cin;
using std::cout;
using std::endl;

int main(int argc, char** argv)
{
	if (argc != 2)
	{
		cout << "usage: " << argv[0] << " <file>" << endl;
		return 1;
	}

	open_table(argv[1]);

	page_t header;
	file_read_page(0, &header);

	cout << "[header page]\n"
		 << "num pages : " << header.file.num_pages << '\n'
		 << "root page number : " << header.file.root_page_number << '\n'
		 << "free page number : " << header.file.free_page_number << endl;

	cout << "\n[file tree]\n"
		 << BPTree::get().to_string() << endl;

	std::vector<pagenum_t> free_pages;
	{
		pagenum_t free_page = header.file.free_page_number;

		while (free_page != NULL_PAGE_NUM)
		{
			page_t free_p;
			file_read_page(free_page, &free_p);

			free_pages.emplace_back(free_page);

			free_page = free_p.node.free_header.next_free_page_number;
		}
	}

	cout << "\n[free page list]\n";
	for (pagenum_t num : free_pages)
		cout << num << ' ';
	cout << endl;

	for (pagenum_t i = 1; i < header.file.num_pages; ++i)
	{
		page_t node;
		file_read_page(i, &node);

		// is free page
		if (std::find(free_pages.begin(), free_pages.end(), i) != free_pages.end())
		{
			cout << "\n[free page " << i << "]\n"
				 << "next free page number : " << node.node.free_header.next_free_page_number << '\n';
		}
		else
		{
			cout << "\n[non-free page " << i << "]\n"
				 << "is leaf : " << node.node.header.is_leaf << '\n'
				 << "num keys : " << node.node.header.num_keys << '\n'
				 << "page a number : " << node.node.header.page_a_number << '\n'
				 << "parent page number : " << node.node.header.parent_page_number << '\n';

			if (node.node.header.is_leaf)
			{
				for (int i = 0; i < PAGE_DATA_IN_PAGE; ++i)
				{
					cout << std::setw(2) << i << " th key : " << node.node.data[i].key << " value : " << node.node.data[i].value << '\n';
				}
			}
			else
			{
				for (int i = 0; i < PAGE_BRANCHES_IN_PAGE; ++i)
				{
					cout << std::setw(3) << i << " th key : " << node.node.branch[i].key << " branch : " << node.node.branch[i].child_page_number << '\n';
				}
			}
		}

		cout << endl;
	}

	return 0;
}
