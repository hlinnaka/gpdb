#
# Makefile for 'limits' tests
#

subdir = src/test/gp_limits
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

pg_regress_call = $(top_builddir)/src/test/regress/pg_regress --init-file=init_file

# Some tests require special settings in postgresql.conf, and cannot therefore
# run as part of the main suite.
check: check-segspace

check-segspace: all
	$(pg_regress_call) --temp-install=./tmp_check --top-builddir=$(top_builddir) --srcdir=$(abs_srcdir) --schedule=$(srcdir)/segspace_schedule --temp-config=segspace-postgresql.conf
