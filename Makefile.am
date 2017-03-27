ACLOCAL_AMFLAGS = -I m4

ROCKSPEC = $(PACKAGE)-$(VERSION)-1.rockspec

$(ROCKSPEC): $(PACKAGE).rockspec
	cat $(PACKAGE).rockspec > $@

EXTRA_DIST = $(PACKAGE).rockspec.in
DISTCLEANFILES = $(PACKAGE).rockspec

rados_la_SOURCES = lua_rados.cc
rados_la_CXXFLAGS = $(LUA_INCLUDE) -Wall -std=c++11
rados_la_LDFLAGS = -module -avoid-version
lib_LTLIBRARIES = rados.la
