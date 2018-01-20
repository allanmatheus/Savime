lib_LTLIBRARIES = librdmap.la

librdmap_la_SOURCES = client.cpp \
		      server.cpp \
		      common.cpp \
		      messages.cpp

librdmap_la_CXXFLAGS = -g -pthread
librdmap_la_LDFLAGS = -g -version-info 1:1:1 -pthread

include_HEADERS = rdmap.h
