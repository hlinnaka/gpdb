#
# Makefile for optimizer
#
# src/backend/gporca/libgpos/src/test/Makefile
#

subdir = src/backend/gporca/libgpos/src/test
top_builddir = ../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

# FIXME: There's no need to link this into the 'postgres' executable. But it's
# needed for the stand-alone test program, 'gporca_test'.
OBJS = CUnittest.o

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
