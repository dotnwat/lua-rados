/**
@module lua-rados
*/
#include <errno.h>

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include <rados/librados.h>

#define LRAD_TRADOS_T "Rados.RadosT"
#define LRAD_TIOCTX_T "Rados.IoctxT"

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
	struct lrad_cluster *cluster = (struct lrad_cluster *)luaL_checkudata(L, pos, LRAD_TRADOS_T);
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

/*
 * Get Ioctx object.
 */
static inline rados_ioctx_t *lrad_checkioctx(lua_State *L, int pos)
{
	return (rados_ioctx_t *)luaL_checkudata(L, pos, LRAD_TIOCTX_T);
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

	cluster = (struct lrad_cluster *)lua_newuserdata(L, sizeof(*cluster));
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

/**
Create an I/O context.
@function open_ioctx
@string name of pool
@return I/O context object or nil on failure
@return errstr, ret if failed
@usage ioctx = cluster:open_ioctx('my_pool')
*/
static int lrad_open_ioctx(lua_State *L)
{
	struct lrad_cluster *cluster = lrad_checkcluster_conn(L, 1);
	const char *pool_name = luaL_checkstring(L, 2);
	rados_ioctx_t *ioctx;
	int ret;

	ioctx = (rados_ioctx_t *)lua_newuserdata(L, sizeof(*ioctx));
	luaL_getmetatable(L, LRAD_TIOCTX_T);
	lua_setmetatable(L, -2);

	ret = rados_ioctx_create(cluster->cluster, pool_name, ioctx);
	if (ret)
		return lrad_pusherror(L, ret);

	/* return the userdata */
	return 1;
}

/*
 * Garbage collect the cluster object, and shutdown only if connected.
 */
static int lrad_cluster_gc(lua_State *L)
{
	struct lrad_cluster *cluster = __lrad_checkcluster(L, 1);

	if (cluster->state == CONNECTED)
		rados_shutdown(cluster->cluster);

	cluster->state = SHUTDOWN;

	return 0;
}

/**
@type Ioctx
*/

/**
Close the I/O context.
@function close
@usage ioctx:close()
*/
static int lrad_ioctx_close(lua_State *L)
{
	rados_ioctx_t *ioctx = lrad_checkioctx(L, 1);

	rados_ioctx_destroy(*ioctx);

	return 0;
}

/**
Get object stat info (size/mtime)
@function stat 
@string oid object name
@return len, mtime, or nil on failure
@return errstr and retval if failed
@usage size, mtime = ioctx:stat('obj3')
*/
static int lrad_ioctx_stat(lua_State *L)
{
	rados_ioctx_t *ioctx = lrad_checkioctx(L, 1);
	const char *oid = luaL_checkstring(L, 2);
	uint64_t len;
	time_t mtime;
	int ret;

	ret = rados_stat(*ioctx, oid, &len, &mtime);

	if (ret)
		return lrad_pusherror(L, ret);

	lua_pushinteger(L, len);
	lua_pushinteger(L, mtime);
	/* return the userdata */
	return 2;
}

/**
Write data to an object.
@function write
@string oid object name
@string buf buffer containing bytes
@int length number of bytes to write
@int offset offset in object from which to write
@return number of bytes written, or nil on error
@return errstr and retval if failed
@usage ioctx:write('obj3', 'data', #'data', 0)
*/
static int lrad_ioctx_write(lua_State *L)
{
	rados_ioctx_t *ioctx = lrad_checkioctx(L, 1);
	const char *oid = luaL_checkstring(L, 2);
	const char *buf = luaL_checkstring(L, 3);
	size_t len = luaL_checkint(L, 4);
	uint64_t off = luaL_checkint(L, 5);
	int ret;

	ret = rados_write(*ioctx, oid, buf, len, off);

	return lrad_pushresult(L, (ret >= 0), ret);
}

/**
Read data from an object.
@function read
@string oid object name
@int length number of bytes read
@int offset offset in object from which to read
@return string with at most `length` bytes, or nil on error
@return errstr and retval if failed
@usage data = ioctx:read('obj3', 1000, 0)
*/
static int lrad_ioctx_read(lua_State *L)
{
	rados_ioctx_t *ioctx = lrad_checkioctx(L, 1);
	const char *oid = luaL_checkstring(L, 2);
	size_t len = luaL_checkint(L, 3);
	uint64_t off = luaL_checkint(L, 4);
	void *ud;
  char *buf;
	lua_Alloc lalloc = lua_getallocf(L, &ud);
	int ret;

	buf = (char *)lalloc(ud, NULL, 0, len);
	if (!buf && len > 0)
		return lrad_pusherror(L, -ENOMEM);

	ret = rados_read(*ioctx, oid, buf, len, off);
	if (ret < 0)
		return lrad_pusherror(L, ret);

	lua_pushlstring(L, buf, ret);

	lalloc(ud, buf, 0, 0);

	return 1;
}

static const luaL_Reg clusterlib_m[] = {
	{"conf_read_file", lrad_conf_read_file},
	{"connect", lrad_connect},
	{"shutdown", lrad_shutdown},
	{"open_ioctx", lrad_open_ioctx},
	{"__gc", lrad_cluster_gc},
	{NULL, NULL}
};

static const luaL_Reg ioctxlib_m[] = {
	{"close", lrad_ioctx_close},
	{"write", lrad_ioctx_write},
	{"read", lrad_ioctx_read},
	{"stat", lrad_ioctx_stat},
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

	/* setup rados_ioctx_t userdata type */
	luaL_newmetatable(L, LRAD_TIOCTX_T);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	luaL_register(L, NULL, ioctxlib_m);
	lua_pop(L, 1);

	luaL_register(L, "rados", radoslib_f);

	return 0;
}
