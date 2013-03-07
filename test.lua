local rados = require "rados"

describe("version", function()
  local major, minor, extra

  setup(function()
    major, minor, extra = rados.version()
  end)

  it("returns three numeric values", function()
    assert.is.number(major)
    assert.is.number(minor)
    assert.is.number(extra)
  end)

  it("returns a non-zero version part", function()
    assert.is_true(major > 0 or minor > 0 or extra > 0)
  end)
end)

describe("create", function()
  it("excepts no id paraemter", function()
    cluster = rados.create()
    assert.is_userdata(cluster)
  end)

  it("excepts nil id paraemter", function()
    cluster = rados.create(nil)
    assert.is_userdata(cluster)
  end)

  it("excepts string id paraemter", function()
    cluster = rados.create('admin')
    assert.is_userdata(cluster)
  end)

  it("throws error on numeric parameter", function()
    assert.error(function() rados.create(0) end)
  end)

  it("throws error on non string id paraemter", function()
    assert.error(function() rados.create({}) end)
  end)
end)

describe("cluster object", function()
  local cluster_new, cluster_conn, cluster_down

  before_each(function()
    cluster_new = rados.create()

    cluster_conn = rados.create()
    cluster_conn:conf_read_file()
    cluster_conn:connect()

    cluster_down = rados.create()
    cluster_down:conf_read_file()
    cluster_down:connect()
    cluster_down:shutdown()
  end)

  describe("connect method", function()
    it("throws error if connected", function()
      assert.error(function() cluster_conn:connect() end)
    end)

    it("throws error if shutdown", function()
      assert.error(function() cluster_down:connect() end)
    end)
  end)

  describe("shutdown method", function()
    it("succeeds if connected", function()
      cluster_conn:shutdown()
    end)

    it("throws error if not connected", function()
      assert.error(function() cluster_new:shutdown() end)
    end)

    it("throws error if shutdown", function()
      assert.error(function() cluster_down:shutdown() end)
    end)
  end)

  describe("open_ioctx method", function()
    it("returns a userdata object", function()
      assert.is_userdata(cluster_conn:open_ioctx('data'))
    end)

    it("throws error without pool name", function()
      assert.error(function() cluster_conn:open_ioctx() end)
    end)

    it("throws error with non-string param", function()
      assert.error(function() cluster_conn:open_ioctx({}) end)
    end)

    it("returns nil for non-existent pools", function()
      assert.is_nil(cluster_conn:open_ioctx(23423432))
      assert.is_nil(cluster_conn:open_ioctx('asdlfk'))
    end)

    it("throws error if not connected", function()
      assert.error(function() cluster_new:open_ioctx('data') end)
    end)

    it("throws error if shutdown", function()
      assert.error(function() cluster_down:open_ioctx('data') end)
    end)
  end)
end)

describe("ioctx object", function()
  local cluster, ioctx

  before_each(function()
    cluster = rados.create()
    cluster:conf_read_file()
    cluster:connect()
    ioctx = cluster:open_ioctx('data')
  end)

  it("is usable if cluster ref disappears", function()
    cluster = nil
    collectgarbage()
    local data = 'wkjeflkwjelfkjwelfkjwef'
    assert.is_equal(#data, ioctx:write('oid', data, #data, 0))
  end)

  describe("write method", function()
    it("returns number of bytes written", function()
      local data = 'wkjeflkwjelfkjwelfkjwef'
      assert.is_equal(#data, ioctx:write('oid', data, #data, 0))
    end)
  end)

  describe("read method", function()
    it("will read what has been written", function()
      local data = 'whoopie doo'
      ioctx:write('oid', data, #data, 0);
      assert.is_equal(data, ioctx:read('oid', #data, 0))
    end)
  end)

  describe("stat method", function()
    it("returns size and last modify time of object", function()
      local data = 'wkjeflkwjelfkjwelfkjwef'
      local length, mtime
      ioctx:write('oid', data, #data, 0);
      length, mtime = ioctx:stat('oid')
      assert.is_equal(#data, length)
    end)
  end)

  describe("setxattr method", function()
    it("throws error with non-string object name", function()
      assert.error(function()
        ioctx:setxattr({}, 'name', 'data', #'data')
      end)
      assert.error(function()
        ioctx:setxattr(nil, 'name', 'data', #'data')
      end)
    end)

    it("throws error with non-string xattr name", function()
      assert.error(function()
        ioctx:setxattr('oid', {}, 'data', #'data')
      end)
      assert.error(function()
        ioctx:setxattr('oid', nil, 'data', #'data')
      end)
    end)

    it("throws error with non-string buffer", function()
      assert.error(function()
        ioctx:setxattr('oid', 'name', {}, 0)
      end)
    end)

    it("throws error with nil buffer", function()
      assert.error(function()
        ioctx:setxattr('oid', 'name', nil, 0)
      end)
    end)

    it("throws error if length is negative", function()
      assert.error(function()
        ioctx:setxattr('oid', 'name', 'data', -1)
      end)
    end)

    it("throws error if length is larger than buffer", function()
      assert.error(function()
        local data = 'data'
        ioctx:setxattr('oid', 'name', data, #data+1)
      end)
    end)

    it("returns zero on success", function()
      local data = 'wkjeflkwjelfkjwelfkjwef'
      local name = 'xattrset'
      local length
      length = ioctx:setxattr('oid', name, data, #data)
      assert.is_equal(0, length)
    end)
  end)

  describe("getxattr method", function()
    it("throws error with non-string object name", function()
      assert.error(function()
        ioctx:getxattr({}, 'name', 'data', #'data')
      end)
      assert.error(function()
        ioctx:getxattr(nil, 'name', 'data', #'data')
      end)
    end)

    it("throws error with non-string xattr name", function()
      assert.error(function()
        ioctx:getxattr('oid', {}, 'data', #'data')
      end)
      assert.error(function()
        ioctx:getxattr('oid', nil, 'data', #'data')
      end)
    end)

    it("will read xattr", function()
      local data = 'wkjeflkwjelfkjwelfkjwef'
      local name = 'xattrget'
      ioctx:setxattr('oid', name, data, #data)
      assert.is_equal(data, ioctx:getxattr('oid',name))
    end)
  end)  
  
  describe("omapset", function()
		
    it("throws error if second arg is not a table", function()
      assert.error(function()
        local kvpairs = "hello" 
        local numpairs = 2 
        ioctx:omapset('oid', kvpairs)
      end)
    end)
    
    it("throws error if table isnt strictly strings", function()
      assert.error(function()
        local kvpairs = { 1,2,3, key1 = "val1"}
        local numpairs = 2 
        ioctx:omapset('oid', kvpairs)
      end)
    end)

    it("returns 0 on success", function()
      local kvpairs = { key1 = "val1", key2 = "val2" }
      local numpairs = 2 
      assert.is_equal(0,ioctx:omapset('oid', kvpairs))
    end)
  end)

  describe("omapget", function()
    
    it("returns 0 on success", function()
      local kvpairs = { key1 = "val1", key2 = "val2" }
      local numpairs = 2 
      assert.is_equal(0,ioctx:omapset('oid', kvpairs))
      local retpairs, retpairnum = ioctx:omapget('oid', "", numpairs)
      assert.is_equal(numpairs, retpairnum)
      assert.is_equal(kvpairs[key1], retpairs[key1])
      assert.is_equal(kvpairs[key2], retpairs[key2])
    end)
  
    it("returns only keys greater than arg2", function()
      local kvpairs = { key1 = "val1", key2 = "val2" }
      local numpairs = 2 
      assert.is_equal(0,ioctx:omapset('oid', kvpairs))
      local retpairs, retpairnum = ioctx:omapget('oid', "key1", numpairs)
      assert.is_equal(numpairs -1 , retpairnum)
      assert.is_equal(kvpairs[key2], retpairs[key1])
    end)

  
  end)

  describe("exec", function()
    it("throws error if length is negative", function()
      assert.error(function()
        ioctx:exec('oid', 'cls', 'method', 'data', -1)
      end)
    end)

    it("throws error if length is larger than buffer", function()
      assert.error(function()
        local data = 'data'
        ioctx:exec('oid', 'cls', 'method', data, #data+1)
      end)
    end)

    it("throws error with non-string object name", function()
      assert.error(function()
        ioctx:exec({}, 'cls', 'method', 'data', #'data')
      end)
      assert.error(function()
        ioctx:exec(nil, 'cls', 'method', 'data', #'data')
      end)
    end)

    it("throws error with non-string class name", function()
      assert.error(function()
        ioctx:exec('oid', {}, 'method', 'data', #'data')
      end)
      assert.error(function()
        ioctx:exec('oid', nil, 'method', 'data', #'data')
      end)
    end)

    it("throws error with non-string method name", function()
      assert.error(function()
        ioctx:exec('oid', 'cls', {}, 'data', #'data')
      end)
      assert.error(function()
        ioctx:exec('oid', 'cls', nil, 'data', #'data')
      end)
    end)

    it("throws error with non-string buffer", function()
      assert.error(function()
        ioctx:exec('oid', 'cls', 'method', {}, #'data')
      end)
    end)

    it("throws error if nil buf and positive length", function()
      assert.error(function()
        ioctx:exec('oid', 'cls', 'method', nil, 1)
      end)
    end)

    pending("pass the echo test", function()
      local indata = 'asldjflkasjdlfkjasdf'
      ret, outdata = ioctx:exec('oid', 'echo', 'echo', indata, #indata)
      assert.is_equal(ret, #indata)
      assert.is_equal(outdata, indata)
    end)
  end)
end)
