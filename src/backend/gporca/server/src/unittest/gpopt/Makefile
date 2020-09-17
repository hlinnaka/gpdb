#
# Makefile for GPORCA unit test program
#
# src/backend/gporca/server/src/unittest/gpopt/Makefile
#

subdir = src/backend/gporca/server/src/unittest/gpopt
top_builddir = ../../../../../../..
include $(top_builddir)/src/Makefile.global

include $(top_srcdir)/src/backend/gporca/gporca.mk

SUBDIRS = base cost csq engine eval mdcache metadata minidump operators search translate xforms

include $(top_srcdir)/src/backend/gporca/server/gporca_test.mk
