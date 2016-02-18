MODULE_big = gps3ext
OBJS =  src/gps3ext.o src/S3ExtWrapper.o src/S3Common.o src/S3Downloader.o \
	src/HTTPFetcher.o src/DownloadPipeline.o src/utils.o \
	lib/http_parser.o lib/ini.o

REGRESS = s3read

PG_CPPFLAGS = -fPIC -Isrc -Ilib -I/usr/include/libxml2 -g
SHLIB_LINK = -lxml2 -lcrypto -lcurl

PGXS := $(shell pg_config --pgxs)
include $(PGXS)
