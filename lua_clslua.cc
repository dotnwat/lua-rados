/**
@module lua-clslua
*/
#include <errno.h>

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include <rados/librados.hpp>
#include <cls_lua/cls_lua_client.hpp>

#include "common.h"

using librados::bufferlist;
using librados::Rados;
using librados::IoCtx;

/**
Execute a script using cls_lua.
@function exec
@param ioctx lua-rados.Ioctx instance
@string oid object name
@string script Lua script
@string handler nam of Lua handler
@string input input buffer
@return retval, output
*/
static int clslua_exec(lua_State *L)
{
  size_t input_len;
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  const char *script = luaL_checkstring(L, 3);
  const char *handler = luaL_checkstring(L, 4);
  const char *input = lua_tolstring(L, 5, &input_len);
  bufferlist *inbl = lrad_newbufferlist(L);
  bufferlist *outbl = lrad_newbufferlist(L);
  int ret;

  if (!input && lua_type(L, 5) != LUA_TNIL)
    return luaL_argerror(L, 5, "expected string or nil");

  if (input)
    inbl->append(input, input_len);

  ret = cls_lua_client::exec(*ioctx, oid, script, handler, *inbl, *outbl, NULL);
  if (ret < 0) {
    inbl->clear();
    outbl->clear();
    lua_pushnil(L);
    lua_pushfstring(L, "%s", strerror(ret));
    lua_pushinteger(L, ret);
    return 3;
  }

  lua_pushinteger(L, ret);
  lua_pushlstring(L, outbl->c_str(), outbl->length());

  inbl->clear();
  outbl->clear();

  return 2;
}

static const luaL_Reg clslualib_f[] = {
  {"exec", clslua_exec},
  {NULL, NULL}
};

LUALIB_API int luaopen_clslua(lua_State *L)
{
  luaL_register(L, "clslua", clslualib_f);
  return 0;
}
