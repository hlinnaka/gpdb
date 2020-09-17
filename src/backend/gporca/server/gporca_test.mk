override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/server/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpos/server/include $(CPPFLAGS)

include $(top_srcdir)/src/backend/common.mk
