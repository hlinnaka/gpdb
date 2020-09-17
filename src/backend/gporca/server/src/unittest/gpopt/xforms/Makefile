#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/xforms/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt/xforms
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CDecorrelatorTest.o \
	CJoinOrderTest.o \
	CSubqueryHandlerTest.o \
	CXformFactoryTest.o \
	CXformTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
