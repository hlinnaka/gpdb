#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/dxl/Makefile
#

subdir = src/backend/gporca/server/src/unittest/dxl
top_builddir = ../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CDXLUtilsTest.o \
	CParseHandlerCostModelTest.o \
	CParseHandlerOptimizerConfigSerializeTest.o \
	CXMLSerializerTest.o \
	CDXLMemoryManagerTest.o \
	CMDProviderTest.o \
	CParseHandlerManagerTest.o \
	CParseHandlerTest.o

SUBDIRS = base statistics

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
