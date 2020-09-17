#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/operators/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt/operators
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CContradictionTest.o \
	CExpressionPreprocessorTest.o \
	CExpressionTest.o \
	CPredicateUtilsTest.o \
	CScalarIsDistinctFromTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
