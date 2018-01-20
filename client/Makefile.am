lib_LTLIBRARIES = libhello.la
libhello_la_SOURCES = ../lib/savime_lib.cpp
libhello_la_LDFLAGS = -version-info 1:1:1

bin_PROGRAMS = savimec
savimec_SOURCES = client.cpp

savimec_LDADD= libhello.la -lpthread ../rdmap/librdmap.a -lrdmacm -libverbs
savimec_LDFLAGS = -static -rpath /usr/local/lib 
