.SUFFIXES: .cc .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cc
TARGET_OBJ:=$(SRCDIR)main.o
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.cc $(SRCDIR)file.cc $(SRCDIR)dbapi.cc $(SRCDIR)page.cc
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cc=.o)

CFLAGS+= -g -fPIC -I $(INC)
CXXFLAGS+= -std=c++1z -Wall -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ) $(STATIC_LIB)
	$(CC) $(CXXFLAGS) -o $@ $< -L $(LIBS) -lbpt

$(SRCDIR)%.o: $(SRCDIR)%.cc
	$(CC) $(CXXFLAGS) -o $@ -c $^

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

$(STATIC_LIB): $(OBJS_FOR_LIB)
	ar cr $@ $^
