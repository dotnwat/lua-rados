#ifndef COMMON_H
#define COMMON_H

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include <rados/librados.hpp>

extern librados::bufferlist *lrad_newbufferlist(lua_State *L);

#define LRAD_TIOCTX_T "Rados.IoctxT"

/*
 * Get Ioctx object.
 */
static inline librados::IoCtx *lrad_checkioctx(lua_State *L, int pos)
{
  return (librados::IoCtx *)luaL_checkudata(L, pos, LRAD_TIOCTX_T);
}


#endif
