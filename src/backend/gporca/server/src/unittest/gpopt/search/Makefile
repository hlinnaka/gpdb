#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/search/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt/search
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	COptimizationJobsTest.o \
	CSearchStrategyTest.o \
	CTreeMapTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
