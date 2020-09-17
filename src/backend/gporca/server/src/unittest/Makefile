#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/Makefile
#

subdir = src/backend/gporca/server/src/unittest
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CConstExprEvaluatorForDates.o CSubqueryTestUtils.o CTestUtils.o

SUBDIRS = dxl gpopt

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
