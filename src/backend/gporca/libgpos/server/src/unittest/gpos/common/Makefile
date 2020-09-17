#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/libgpos/server/src/unittest/gpos/common/Makefile
#

subdir = src/backend/gporca/libgpos/server/src/unittest/gpos/common
top_builddir = ../../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CAutoPTest.o CBitSetIterTest.o CDoubleTest.o CHashMapIterTest.o \
	CHashSetTest.o CStackTest.o CAutoRefTest.o CBitSetTest.o \
	CDynamicPtrArrayTest.o CHashMapTest.o CListTest.o \
	CSyncHashtableTest.o CAutoRgTest.o CBitVectorTest.o \
	CEnumSetTest.o CHashSetIterTest.o  CRefCountTest.o \
	CSyncListTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
