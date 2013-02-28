extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include <rados/librados.hpp>
#include "common.h"

using librados::bufferlist;

#define LRAD_BL_T "Rados.Bufferlist"

/*
 * Garbage collect a bufferlist in the heap
 */
static int bl_gc(lua_State *L)
{
  bufferlist **pbl = (bufferlist **)luaL_checkudata(L, 1, LRAD_BL_T);
  if (pbl && *pbl)
    delete *pbl;
  return 0;
}

/*
 * Allocate a new bufferlist in the heap
 */
bufferlist *lrad_newbufferlist(lua_State *L)
{
  if (luaL_newmetatable(L, LRAD_BL_T)) {
    lua_pushcfunction(L, bl_gc);
    lua_setfield(L, -2, "__gc");
  }

  bufferlist *bl = new bufferlist();
  bufferlist **pbl = (bufferlist **)lua_newuserdata(L, sizeof(*pbl));
  *pbl = bl;

  lua_pushvalue(L, 1); /* copy metatable */
  lua_setmetatable(L, -2);
  lua_remove(L, 1); /* remove orig metatable */

  return bl;
}
