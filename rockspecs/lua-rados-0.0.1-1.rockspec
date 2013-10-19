package="lua-rados"
version="0.0.1-1"
source = {
  url = "https://github.com/noahdesu/lua-rados/archive/v0.0.1.tar.gz",
  dir = "lua-rados-0.0.1"
}
description = {
  summary = "Lua bindings to RADOS",
  detailed = [[]],
  homepage = "http://github.com/noahdesu/lua-rados/",
  license = "MIT"
}
dependencies = {
  "lua >= 5.1"
}
build = {
  type = "command",
  build_command = "./bootstrap.sh && LUA=$(LUA) CPPFLAGS=-I$(LUA_INCDIR) ./configure --prefix=$(PREFIX) --libdir=$(LIBDIR) --datadir=$(LUADIR) && make clean && make",
  install_command = "make install"
}
