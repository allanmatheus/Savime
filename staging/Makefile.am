#AM_CXXFLAGS = -pg
#AM_LDFLAGS = -pg

lib_LTLIBRARIES = libstaging.la

libstaging_la_SOURCES = ../lib/savime_lib.cpp \
			../util/chann.h \
			../lib/protocol.h \
			staging.h \
			staging.cpp \
			rdma_utils.h \
			rdma_utils.cpp
libstaging_la_LDFLAGS = -version-info 1:1:1

bin_PROGRAMS = staging ststaging

staging_SOURCES = server.cpp
staging_LDADD = libstaging.la \
		-lpthread \
		../mapped_memory/libmappedmemory.a \
		../rdmap/librdmap.a \
		-lrdmacm \
		-libverbs
staging_LDFLAGS = -static -rpath /usr/local/lib 

ststaging_SOURCES = stserver.cpp
ststaging_LDADD = libstaging.la \
		-lpthread \
		../mapped_memory/libmappedmemory.a \
		../rdmap/librdmap.a \
		-lrdmacm \
		-libverbs
ststaging_LDFLAGS = -static -rpath /usr/local/lib 

check_PROGRAMS = test_buffer test_files

test_buffer_SOURCES = test_buffer.cpp
test_buffer_LDADD = libstaging.la \
		   -lpthread \
		   ../mapped_memory/libmappedmemory.a \
		   ../rdmap/librdmap.a \
		   -lrdmacm \
		   -libverbs
test_buffer_LDFLAGS = -static -rpath /usr/local/lib 

test_files_SOURCES = test_files.cpp
test_files_LDADD = libstaging.la \
		   -lpthread \
		   ../mapped_memory/libmappedmemory.a \
		   ../rdmap/librdmap.a \
		   -lrdmacm \
		   -libverbs
test_files_LDFLAGS = -static -rpath /usr/local/lib 
