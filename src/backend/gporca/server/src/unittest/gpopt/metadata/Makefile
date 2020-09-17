#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/metadata/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt/metadata
top_builddir = ../../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

OBJS=	CColumnDescriptorTest.o \
	CIndexDescriptorTest.o \
	CNameTest.o \
	CPartConstraintTest.o \
	CTableDescriptorTest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
