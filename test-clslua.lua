local rados = require "rados"
local clslua = require "clslua"

describe("clslua.exec", function()
  local ioctx

  before_each(function()
    local cluster = rados.create()
    cluster:conf_read_file()
    cluster:connect()
    ioctx = cluster:open_ioctx('data')
  end)

  it("passes the echo test", function()
    local script = [[
      function echo(input, output)
        output:append(input:str())
        return #input
      end
      cls.register(echo)]]

    input = 'flap jacks'
    ret, output = clslua.exec(ioctx, 'oid', script, 'echo', input)

    assert.is_equal(output, input)
    assert.is_equal(ret, #input)
  end)
end)
