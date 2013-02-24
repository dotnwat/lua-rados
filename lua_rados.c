/**
@module lua-rados
*/
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <rados/librados.h>

/**
Get the version of librados.
@function version
@return major
@return minor
@return extra
 */
static int lrad_version(lua_State *L)
{
	int major, minor, extra;

	rados_version(&major, &minor, &extra);

	lua_pushinteger(L, major);
	lua_pushinteger(L, minor);
	lua_pushinteger(L, extra);

	return 3;
}

static const luaL_Reg radoslib_f[] = {
	{"version", lrad_version},
	{NULL, NULL}
};

LUALIB_API int luaopen_rados(lua_State *L)
{
	luaL_register(L, "rados", radoslib_f);
	return 0;
}
