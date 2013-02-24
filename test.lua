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
