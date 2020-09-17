#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/minidump/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt/minidump
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CAggTest.o \
	CConstTblGetTest.o \
	CExternalTableTest.o \
	CMissingStatsTest.o \
	CSubqueryTest.o \
	CArrayExpansionTest.o \
	CCTETest.o \
	CICGTest.o \
	CMultilevelPartitionTest.o \
	CTVFTest.o \
	CBitmapTest.o \
	CDirectDispatchTest.o \
	CJoinOrderDPTest.o \
	CPhysicalParallelUnionAllTest.o \
	CWindowTest.o \
	CCastTest.o \
	CDMLTest.o \
	CMiniDumperDXLTest.o \
	CPruneColumnsTest.o \
	CCollapseProjectTest.o \
	CEscapeMechanismTest.o \
	CMinidumpWithConstExprEvaluatorTest.o \
	CPullUpProjectElementTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
