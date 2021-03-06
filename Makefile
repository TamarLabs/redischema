#set environment variable RM_INCLUDE_DIR to the location of redisredischema.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=./
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=rmutil
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -g -fPIC -lc -lm -O2 -std=gnu99
CC=gcc

all: redischema.so

rmutil:
	$(MAKE) -C $(RMUTIL_LIBDIR)

redischema.so: redischema.o jsmn.o
	$(LD) -o $@ redischema.o jsmn.o $(SHOBJ_LDFLAGS) $(LIBS) -lc

jsmn.o: jsmn.c jsmn.h
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -rf *.xo *.so *.o
	rm -rf ./$(RMUTIL_LIBDIR)/*.so ./$(RMUTIL_LIBDIR)/*.o ./$(RMUTIL_LIBDIR)/*.a
