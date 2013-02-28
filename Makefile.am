ACLOCAL_AMFLAGS = -I m4

rados_la_SOURCES = lua_rados.cc lua_bufferlist.cc
rados_la_CXXFLAGS = $(LUA_INCLUDE) -Wall
rados_la_LDFLAGS = -module -avoid-version
lib_LTLIBRARIES = rados.la

clslua_la_SOURCES = lua_clslua.cc lua_bufferlist.cc
clslua_la_CXXFLAGS = $(LUA_INCLUDE) -Wall
clslua_la_LDFLAGS = -module -avoid-version
clslua_la_LIBADD = -lcls_lua_client
lib_LTLIBRARIES += clslua.la
