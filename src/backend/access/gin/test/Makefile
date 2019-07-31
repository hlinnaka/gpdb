top_builddir=../../../../..
subdir=src/backend/access/gin

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/Makefile.mock


TARGETS=ginpostinglist


ginpostinglist.t: $(top_srcdir)/$(subdir)/ginpostinglist.o
	make -C $(top_srcdir)/$(subdir)/ && \
	$(CC) $(CFLAGS) $(LDFLAGS) $(CMOCKERY_OBJS) $(CPPFLAGS) \
		$(top_srcdir)/$(subdir)/ginpostinglist.o \
		ginpostinglist_fakes.c \
		ginpostinglist_test.c \
		-o ginpostinglist.t


check: ginpostinglist.t
	./ginpostinglist.t


clean:
	@rm -f *.o *.t
