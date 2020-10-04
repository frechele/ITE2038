#include "disk.h"

#include <fcntl.h>

static int fid;

int open_file(const char* filename)
{
    open(filename, O_RDWR | O_APPEND | O_CREAT);
}
