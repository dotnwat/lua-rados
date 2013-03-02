ACLOCAL_AMFLAGS = -I m4

liblua_bl_la_SOURCES = lua_bufferlist.cc
liblua_bl_la_CXXFLAGS = $(LUA_INCLUDE) -Wall -g
lib_LTLIBRARIES = liblua_bl.la

rados_la_SOURCES = lua_rados.cc
rados_la_CXXFLAGS = $(LUA_INCLUDE) -Wall -g
rados_la_LDFLAGS = -module -avoid-version
rados_la_LIBADD = liblua_bl.la
lib_LTLIBRARIES += rados.la

clslua_la_SOURCES = lua_clslua.cc
clslua_la_CXXFLAGS = $(LUA_INCLUDE) -Wall -g
clslua_la_LDFLAGS = -module -avoid-version
clslua_la_LIBADD = -lcls_lua_client liblua_bl.la
lib_LTLIBRARIES += clslua.la
