.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.c
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.c $(SRCDIR)disk.c $(SRCDIR)dbapi.c
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.c=.o)

CFLAGS+= -g -fPIC -I $(INC)
CXXFLAGS+= -std=c++1z -g -fPIC -I $(INC)

TARGET=main

all: $(OBJS_FOR_LIB) $(TARGET)

$(TARGET): $(TARGET_OBJ)
	make static_library
	$(CC) $(CXXFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

build_library:
	$(foreach src, $(SRCS_FOR_LIB), $(CC) $(CXXFLAGS) -o $(file:.c=.o) -c $(file);)

library:
	$(CC) -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
