--
-- usage: mk-dset.lua pool prefix objsize, numobjs
--
rados = require "rados"
randstr = require "randstr"

pool_name = arg[1]
prefix = arg[2]
obj_size = arg[3]
num_objs = arg[4]

cluster = rados.create()
cluster:conf_read_file()
cluster:connect()

ioctx = cluster:open_ioctx(pool_name)

print("Summary")
print(" pool:", pool_name)
print(" prefix:", prefix)
print(" dataset size:", obj_size * num_objs)
print(" object size:", obj_size)
print(" num objects:", num_objs)

first_obj = nil
obj_data = randstr(obj_size)
for i=0, num_objs-1 do
  obj_name = prefix .. ".obj." .. i
  if i == 0 then first_obj = obj_name end
  ioctx:write(obj_name, obj_data, #obj_data, 0)
  if i % 10 == 0 then
    print("finished writing object: " .. obj_name)
  end
end
