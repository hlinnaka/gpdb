MODULE_big = gps3ext
OBJS =  src/gps3ext.o src/S3ExtWrapper.o src/S3Common.o src/s3operations.o \
	src/HTTPFetcher.o src/DownloadPipeline.o src/utils.o \
	lib/ini.o

REGRESS = signing s3read 


# Note: we require curl 7.17 higher (why? What's the limiting factor?)


PG_CPPFLAGS = -fPIC -Isrc -Ilib -I/usr/include/libxml2 -g
SHLIB_LINK = -lxml2 -lcrypto -lcurl

PGXS := $(shell pg_config --pgxs)
include $(PGXS)
