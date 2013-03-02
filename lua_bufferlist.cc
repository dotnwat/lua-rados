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

static const luaL_Reg bllib_m[] = {
  {"__gc", bl_gc},
  {NULL, NULL}
};

/*
 * Allocate a new bufferlist in the heap
 */
bufferlist *lrad_newbufferlist(lua_State *L)
{
  if (luaL_newmetatable(L, LRAD_BL_T)) {
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_register(L, NULL, bllib_m);
    lua_pop(L, 1);
  }

  bufferlist *bl = new bufferlist();
  bufferlist **pbl = (bufferlist **)lua_newuserdata(L, sizeof(*pbl));
  *pbl = bl;

  luaL_getmetatable(L, LRAD_BL_T);
  lua_setmetatable(L, -2);

  return bl;
}
