/**
@module lua-rados
*/
#include <errno.h>

extern "C" {
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
}

#include <string>
#include <map>
#include <rados/librados.hpp>

using librados::bufferlist;
using librados::Rados;
using librados::IoCtx;

#define LRAD_TRADOS_T "Rados.RadosT"
#define LRAD_TIOCTX_T "Rados.IoctxT"
#define LRAD_BL_T "Rados.Bufferlist"
#define LRAD_SBM_T "Rados.StrBLMap"

typedef std::map<std::string, bufferlist> strblmap;

static char reg_key_rados_refs;

typedef enum {
  CREATED,
  CONNECTED,
  SHUTDOWN
} cluster_state_t;

struct lrad_cluster {
  Rados rados;
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
static inline IoCtx *lrad_checkioctx(lua_State *L, int pos)
{
  return (IoCtx *)luaL_checkudata(L, pos, LRAD_TIOCTX_T);
}

/*
 * Push nil-error protocol values
 */
static int lrad_pusherror(lua_State *L, int ret)
{
  lua_pushnil(L);
  lua_pushfstring(L, "%s", strerror(ret));
  lua_pushinteger(L, ret);
  return 3;
}

/*
 * Push return value or error if not ok
 */
static int lrad_pushresult(lua_State *L, int ok, int ret)
{
  if (!ok)
    return lrad_pusherror(L, ret);
  lua_pushinteger(L, ret);
  return 1;
}

/*
 * Garbage collect a bufferlist in the heap
 */
static int lrad_bl_gc(lua_State *L)
{
  bufferlist **pbl = (bufferlist **)luaL_checkudata(L, 1, LRAD_BL_T);
  if (pbl && *pbl)
    delete *pbl;
  return 0;
}

/*
 * Allocate a new bufferlist in the heap
 */
static bufferlist *lrad_newbufferlist(lua_State *L)
{
  bufferlist *bl = new bufferlist();
  bufferlist **pbl = (bufferlist **)lua_newuserdata(L, sizeof(*pbl));
  *pbl = bl;

  luaL_getmetatable(L, LRAD_BL_T);
  lua_setmetatable(L, -2);

  return bl;
}

/*
 * Garbage collect a string buffer list map in the heap
 */
static int lrad_sbm_gc(lua_State *L)
{
  strblmap **sbmp = (strblmap **)luaL_checkudata(L, 1, LRAD_SBM_T);
  if (sbmp && *sbmp)
    delete *sbmp;
  return 0;
}

/*
 * Allocate a new string buffer list map in the heap
 */
static strblmap *lrad_newsbm(lua_State *L)
{
  strblmap *pairs = new strblmap;
  strblmap **sbmp = (strblmap **)lua_newuserdata(L, sizeof(*sbmp));
  *sbmp = pairs;

  luaL_getmetatable(L, LRAD_SBM_T);
  lua_setmetatable(L, -2);

  return pairs;
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

  ret = cluster->rados.init(id);
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

  ret = cluster->rados.conf_read_file(conf_file);

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

  ret = cluster->rados.connect();
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

  cluster->rados.shutdown();

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
  IoCtx *ioctx;
  int ret;

  ioctx = (IoCtx *)lua_newuserdata(L, sizeof(*ioctx));
  luaL_getmetatable(L, LRAD_TIOCTX_T);
  lua_setmetatable(L, -2);

  ret = cluster->rados.ioctx_create(pool_name, *ioctx);
  if (ret)
    return lrad_pusherror(L, ret);

  /* record IoCtx -> Rados reference in weak key table */
  lua_pushlightuserdata(L, &reg_key_rados_refs);
  lua_gettable(L, LUA_REGISTRYINDEX);
  assert(!lua_isnil(L, -1));
  assert(lua_type(L, -1) == LUA_TTABLE);
  lua_pushvalue(L, -2); /* key = ioctx */
  lua_pushvalue(L, 1);  /* value = cluster */
  lua_settable(L, -3);
  lua_pop(L, 1);

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
    cluster->rados.shutdown();

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
  IoCtx *ioctx = lrad_checkioctx(L, 1);

  ioctx->close();

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
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  uint64_t len;
  time_t mtime;
  int ret;

  ret = ioctx->stat(oid, &len, &mtime);

  if (ret)
    return lrad_pusherror(L, ret);

  lua_pushinteger(L, len);
  lua_pushinteger(L, mtime);

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
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  const char *buf = luaL_checkstring(L, 3);
  size_t len = luaL_checkint(L, 4);
  uint64_t off = luaL_checkint(L, 5);
  bufferlist *bl = lrad_newbufferlist(L);
  int ret;

  bl->append(buf, len);

  ret = ioctx->write(oid, *bl, len, off);

  bl->clear();

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
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  size_t len = luaL_checkint(L, 3);
  uint64_t off = luaL_checkint(L, 4);
  bufferlist *bl = lrad_newbufferlist(L);
  int ret;

  ret = ioctx->read(oid, *bl, len, off);
  if (ret < 0)
    return lrad_pusherror(L, ret);

  if (bl->length() > len)
    return lrad_pusherror(L, -ERANGE);

  lua_pushlstring(L, bl->c_str(), bl->length());

  bl->clear();

  return 1;
}

/**
Set xattr name on object
@function setxattr
@string oid object name
@string name xattr name
@string buf value to set xattr name to
@int size of buf
@return 0 on success, or nil on error
@return errstr and retval if failed
@usage ioctx:setxattr('obj3', 'name', 'data', #'data')
*/
static int lrad_ioctx_setxattr(lua_State *L)
{
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  const char *name = luaL_checkstring(L, 3);
  size_t buflen, len = luaL_checkint(L, 5);
  const char* buf = lua_tolstring(L, 4, &buflen);
  bufferlist *bl = lrad_newbufferlist(L);
  int ret;

  if( !buf ){
    return luaL_argerror(L, 4, "expected string");
  }

  luaL_argcheck(L, len >= 0, 5, "invalid length");
  luaL_argcheck(L, buflen >= len, 5, "length longer than buffer");

  bl->append(buf, len);

  ret = ioctx->setxattr(oid, name, *bl);

  bl->clear();

  return lrad_pushresult(L, (ret >= 0), ret);
}

/**
Get xattr name from object.
@function getxattr
@string oid object name
@string name xattr name
@return string containing xattr name
@return errstr and retval if failed
@usage data = ioctx:getxattr('obj3', 'name')
*/
static int lrad_ioctx_getxattr(lua_State *L)
{
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  const char *name = luaL_checkstring(L, 3);
  bufferlist *bl = lrad_newbufferlist(L);
  int ret;

  ret = ioctx->getxattr(oid, name, *bl );
  if (ret < 0)
    return lrad_pusherror(L, ret);

  lua_pushlstring(L, bl->c_str(), bl->length());

  bl->clear();

  return 1;
}


/**
Set omap values for oid
@function omapset
@string oid object name
@table pairs [key,value] pairs to be set
@return 0 on success, or nil on error
@return errstr and retval if failed
@usage ioctx:omapset('obj3', 'pairs')
*/
static int lrad_ioctx_omapset(lua_State *L)
{
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  strblmap *pairs;
  int ret;

  if (lua_type(L, 3) != LUA_TTABLE)
    return luaL_error(L, "omapset expects a table for the second argument");

  pairs = lrad_newsbm(L);
  lua_pushnil(L);
  while (lua_next(L, 3) != 0){
    size_t valLength;
    const char *keyTemp = luaL_checkstring(L, -2);
    const char *valTemp = luaL_checklstring(L, -1, &valLength);
    bufferlist bl;

    bl.append(valTemp, valLength);
    (*pairs)[keyTemp] = bl;
    lua_pop(L, 1);
  }
  ret = ioctx->omap_set(oid, *pairs);
  return lrad_pushresult(L, (ret >= 0), ret);
}

/**
Get omap values for oid
@function omapget
@string oid object name
@string after list no keys smaller than after
@int maxret maximum number of key/val pairs to retrieve
@return map, numpairs or nil on failure
@return errstr and retval if failed
@usage map, numpairs  = ioctx:omapget('obj3', 'after', 'maxret')
*/
static int lrad_ioctx_omapget(lua_State *L)
{
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  const char *after = luaL_checkstring(L, 3);
  size_t maxret = luaL_checkint(L, 4);
  strblmap *pairs = lrad_newsbm(L);
  int ret;

  ret = ioctx->omap_get_vals(oid, after, maxret, pairs);
  if (ret < 0)
    return lrad_pusherror(L, ret);

  std::map<std::string, bufferlist>::iterator iter;
  lua_newtable(L);
  size_t pairsProcessed = 0;
  for(iter = pairs->begin(); iter != pairs->end(); iter++){
    const std::string *key = &(iter->first);
    bufferlist *bl = &(iter->second);
    lua_pushlstring(L, key->c_str(), key->length());
    lua_pushlstring(L, bl->c_str(), bl->length());
    lua_settable(L,-3);
    pairsProcessed++;
  }
  lua_pushinteger(L, pairsProcessed);
  return 2;
}

/**
Execute an OSD class method on an object.
@function exec
@string oid the name of the object
@string class the name of the class
@string method the name of the method
@string buf buffer containing method input
@int length length of input buffer
@return retval, output on success, else nil
@return strerr, errval on failure
@usage ret, reply = ioctx:exec('oid', 'cls', 'func', input, #input)
*/
static int lrad_ioctx_exec(lua_State *L)
{
  IoCtx *ioctx = lrad_checkioctx(L, 1);
  const char *oid = luaL_checkstring(L, 2);
  const char *cls = luaL_checkstring(L, 3);
  const char *method = luaL_checkstring(L, 4);
  size_t buflen, len = luaL_checkint(L, 6);
  const char *buf = lua_tolstring(L, 5, &buflen);
  bufferlist *inbl = lrad_newbufferlist(L);
  bufferlist *outbl = lrad_newbufferlist(L);
  int ret;

  if (!buf) {
    if (lua_type(L, 5) != LUA_TNIL)
      return luaL_argerror(L, 5, "expected string or nil");
    buflen = 0;
  }

  luaL_argcheck(L, len >= 0, 6, "invalid length");
  luaL_argcheck(L, buflen >= len, 6, "length longer than buffer");

  if (buf)
    inbl->append(buf, len);

  ret = ioctx->exec(oid, cls, method, *inbl, *outbl);
  if (ret < 0) {
    inbl->clear();
    outbl->clear();
    return lrad_pusherror(L, ret);
  }

  lua_pushinteger(L, ret);
  lua_pushlstring(L, outbl->c_str(), outbl->length());

  inbl->clear();
  outbl->clear();

  return 2;
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
  {"setxattr", lrad_ioctx_setxattr},
  {"getxattr", lrad_ioctx_getxattr},
  {"omapset", lrad_ioctx_omapset},
  {"omapget", lrad_ioctx_omapget},
  {"exec", lrad_ioctx_exec},
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

  /* setup bufferlist userdata type */
  luaL_newmetatable(L, LRAD_BL_T);
  lua_pushcfunction(L, lrad_bl_gc);
  lua_setfield(L, -2, "__gc");
  lua_pop(L, 1);

  /* setup string bufferlist map userdata type */
  luaL_newmetatable(L, LRAD_SBM_T);
  lua_pushcfunction(L, lrad_sbm_gc);
  lua_setfield(L, -2, "__gc");
  lua_pop(L, 1);

  /* weak table to protect IoCtx -> Rados refs */
  lua_pushlightuserdata(L, &reg_key_rados_refs);
  lua_newtable(L);
  lua_pushstring(L, "k");
  lua_setfield(L, -2, "__mode");
  lua_pushvalue(L, -1);
  lua_setmetatable(L, -2);
  lua_settable(L, LUA_REGISTRYINDEX);

  luaL_register(L, "rados", radoslib_f);

  return 0;
}
