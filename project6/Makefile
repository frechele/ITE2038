.SUFFIXES: .cpp .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.cpp
TARGET_OBJ:=$(SRCDIR)main.o
TEST_TARGET_SRC:=$(SRCDIR)test.cpp
TEST_TARGET_OBJ:=$(SRCDIR)test.o
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.cpp $(SRCDIR)file.cpp $(SRCDIR)dbapi.cpp \
				$(SRCDIR)page.cpp $(SRCDIR)buffer.cpp $(SRCDIR)table.cpp \
				$(SRCDIR)lock.cpp $(SRCDIR)xact.cpp $(SRCDIR)log.cpp\
				$(SRCDIR)recovery.cpp
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cpp=.o)

CFLAGS+= -g -fPIC -I $(INC)
CXXFLAGS+= -std=c++1z -Wall  -g -fPIC -I $(INC)

TARGET=main
TEST_TARGET=test

all: $(TARGET) $(TEST_TARGET)

$(TARGET): $(TARGET_OBJ) $(STATIC_LIB)
	$(CC) $(CXXFLAGS) -pthread -o $@ $< -L $(LIBS) -lbpt

$(TEST_TARGET): $(TEST_TARGET_OBJ) $(STATIC_LIB)
	$(CC) $(CXXFLAGS) -pthread -o $@ $< -L $(LIBS) -lbpt

$(SRCDIR)%.o: $(SRCDIR)%.cpp
	$(CC) $(CXXFLAGS) -o $@ -c $^

clean:
	rm $(TARGET) $(TARGET_OBJ) $(TEST_TARGET) $(TEST_TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

$(STATIC_LIB): $(OBJS_FOR_LIB)
	ar cr $@ $^
