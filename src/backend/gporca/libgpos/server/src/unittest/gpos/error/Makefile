#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/libgpos/server/src/unittest/gpos/error/Makefile
#

subdir = src/backend/gporca/libgpos/server/src/unittest/gpos/error
top_builddir = ../../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CConstExprEvaluatorForDates.o CSubqueryTestUtils.o CTestUtils.o

OBJS=	CErrorHandlerTest.o CExceptionTest.o CLoggerTest.o 	CMessageRepositoryTest.o \
	CMessageTableTest.o CMessageTest.o CMiniDumperTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
