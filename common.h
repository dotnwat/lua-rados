#ifndef COMMON_H
#define COMMON_H

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include <rados/librados.hpp>

extern librados::bufferlist *lrad_newbufferlist(lua_State *L);

#endif
