#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/base/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt/base
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CColRefSetIterTest.o \
	CColumnFactoryTest.o \
	CDistributionSpecTest.o \
	CFunctionalDependencyTest.o \
	CMaxCardTest.o \
	CRangeTest.o \
	CColRefSetTest.o \
	CConstraintTest.o \
	CEquivalenceClassesTest.o \
	CKeyCollectionTest.o \
	COrderSpecTest.o \
	CStateMachineTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
