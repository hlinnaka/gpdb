# Makefile for optimizer
#
# src/backend/gporca/Makefile
#

subdir = src/backend/gporca
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

BACKEND_SUBDIRS = libgpos libgpdbcost libnaucrates libgpopt
SUBDIRS     = $(BACKEND_SUBDIRS) server libgpos/server

include $(top_srcdir)/src/backend/common.mk

SUBDIROBJS = $(BACKEND_SUBDIRS:%=%/$(subsysfilename))
ALL_SUBDIROBJS = $(SUBDIRS:%=%/$(subsysfilename))

gporca_test: $(ALL_SUBDIROBJS)
	$(CXX) $(CXXFLAGS) $(call expand_subsys,$(ALL_SUBDIROBJS)) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(LIBS) -o $@

unittest-check: gporca_test
	./gporca_prove --schedule=tap-schedule.txt

clean:
	rm -f gporca_test
