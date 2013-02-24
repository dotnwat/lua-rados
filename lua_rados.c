/**
@module lua-rados
*/
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <rados/librados.h>

#define LRAD_TRADOS_T "Rados.RadosT"

typedef enum {
	CREATED,
	CONNECTED,
	SHUTDOWN
} cluster_state_t;

struct lrad_cluster {
	rados_t cluster;
	cluster_state_t state;
};

/*
 * Get cluster object at stack index.
 */
static inline struct lrad_cluster *__lrad_checkcluster(lua_State *L, int pos)
{
	struct lrad_cluster *cluster = luaL_checkudata(L, pos, LRAD_TRADOS_T);
	return cluster;
}

/*
 * Get non-shutdown cluster object at stack index. Error if had shutdown.
 */
static inline struct lrad_cluster *lrad_checkcluster(lua_State *L, int pos)
{
	struct lrad_cluster *cluster = __lrad_checkcluster(L, pos);

	if (cluster->state == SHUTDOWN)
		luaL_argerror(L, pos, "cannot reuse shutdown cluster handle");

	return cluster;
}

/*
 * Get connected cluster object at stack index. Error if not connected.
 */
static inline struct lrad_cluster *lrad_checkcluster_conn(lua_State *L, int pos)
{
	struct lrad_cluster *cluster = lrad_checkcluster(L, pos);

	if (cluster->state != CONNECTED)
		luaL_argerror(L, pos, "not connected to cluster");

	return cluster;
}

static int lrad_pusherror(lua_State *L, int ret)
{
	lua_pushnil(L);
	lua_pushfstring(L, "%s", strerror(ret));
	lua_pushinteger(L, ret);
	return 3;
}

static int lrad_pushresult(lua_State *L, int ok, int ret)
{
	if (!ok)
		return lrad_pusherror(L, ret);
	lua_pushinteger(L, ret);
	return 1;
}

/**
Get the version of librados.
@function version
@return major version
@return minor version
@return extra version
@usage major, minor, extra = rados.version()
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

/**
Create handle for communicating with RADOS cluster.
@function create
@string id the user to connect as (optional, or nil)
@return cluster handle object on success, nil otherwise
@return error message and retval if failed
@usage cluster = rados.create()
@usage cluster = rados.create('admin')
*/
static int lrad_create(lua_State *L)
{
	const char *id = NULL;
	struct lrad_cluster *cluster;
	int ret;

	if (lua_gettop(L) > 0 && lua_type(L, 1) != LUA_TNIL) {
		luaL_checktype(L, 1, LUA_TSTRING);
		id = lua_tostring(L, 1);
	}

	cluster = lua_newuserdata(L, sizeof(*cluster));
	cluster->state = CREATED;

	luaL_getmetatable(L, LRAD_TRADOS_T);
	lua_setmetatable(L, -2);

	ret = rados_create(&cluster->cluster, id);
	if (ret)
		return lrad_pusherror(L, ret);

	/* return the userdata */
	return 1;
}

/**
@type Cluster
*/

/**
Configure the cluster handle using a Ceph config file.
@function conf_read_file
@string file path to configuration file (optional, or nil)
@return 0 on success, nil otherwise
@return error message and retval if failed
@usage cluster:conf_read_file()
@usage cluster:conf_read_file('/path/to/ceph.conf')
*/
static int lrad_conf_read_file(lua_State *L)
{
	struct lrad_cluster *cluster = lrad_checkcluster(L, 1);
	const char *conf_file = NULL;
	int ret;

	if (lua_gettop(L) > 1 && lua_type(L, 2) != LUA_TNIL) {
		luaL_checktype(L, 2, LUA_TSTRING);
		conf_file = lua_tostring(L, 2);
	}

	ret = rados_conf_read_file(cluster->cluster, conf_file);

	return lrad_pushresult(L, (ret == 0), ret);
}

/**
Connect to the cluster.
@function connect
@return 0 on success, nil otherwise
@return error message and retval if failed
@usage cluster:connect()
@usage status, errstr, ret = cluster:connect()
*/
static int lrad_connect(lua_State *L)
{
	struct lrad_cluster *cluster = lrad_checkcluster(L, 1);
	int ret;

	if (cluster->state == CONNECTED)
		luaL_argerror(L, 1, "already connected to cluster");

	ret = rados_connect(cluster->cluster);
	if (!ret)
		cluster->state = CONNECTED;

	return lrad_pushresult(L, (ret == 0), ret);
}

/**
Disconnect from the cluster.
@function shutdown
@usage cluster:shutdown()
*/
static int lrad_shutdown(lua_State *L)
{
	struct lrad_cluster *cluster = lrad_checkcluster_conn(L, 1);

	rados_shutdown(cluster->cluster);

	cluster->state = SHUTDOWN;

	return 0;
}

static int lrad_cluster_gc(lua_State *L)
{
	struct lrad_cluster *cluster = __lrad_checkcluster(L, 1);

	if (cluster->state == CONNECTED)
		rados_shutdown(cluster->cluster);

	cluster->state = SHUTDOWN;

	return 0;
}

static const luaL_Reg clusterlib_m[] = {
	{"conf_read_file", lrad_conf_read_file},
	{"connect", lrad_connect},
	{"shutdown", lrad_shutdown},
	{"__gc", lrad_cluster_gc},
	{NULL, NULL}
};

static const luaL_Reg radoslib_f[] = {
	{"version", lrad_version},
	{"create", lrad_create},
	{NULL, NULL}
};

LUALIB_API int luaopen_rados(lua_State *L)
{
	/* setup rados_t userdata type */
	luaL_newmetatable(L, LRAD_TRADOS_T);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_register(L, NULL, clusterlib_m);
	lua_pop(L, 1);

	luaL_register(L, "rados", radoslib_f);

	return 0;
}
