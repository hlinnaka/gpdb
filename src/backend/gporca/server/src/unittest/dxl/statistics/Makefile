#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/dxl/statistics/Makefile
#

subdir = src/backend/gporca/server/src/unittest/dxl/statistics
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CBucketTest.o \
	CCardinalityTestUtils.o \
	CFilterCardinalityTest.o \
	CHistogramTest.o \
	CJoinCardinalityTest.o \
	CMCVTest.o \
	CPointTest.o \
	CStatisticsTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
