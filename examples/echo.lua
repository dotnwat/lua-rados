rados = require "rados"
clslus = require "clslua"

pool_name = arg[1]
prefix = arg[2]

cluster = rados.create()
cluster:conf_read_file()
cluster:connect()
ioctx = cluster:open_ioctx(pool_name)

script = [[
function echo(input, output)
  output:append(input:str())
  return #input
end
cls.register(echo)]]

input = 'flap jacks'
ret, output = clslua.exec(ioctx, prefix .. '.echo', script, 'echo', input)

assert(ret == #input)
assert(output == input)
