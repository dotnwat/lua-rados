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

describe("connect", function()
  it("connects", function()
    cluster = rados.create()
    cluster:conf_read_file()
    assert(cluster:connect())
  end)
end)
