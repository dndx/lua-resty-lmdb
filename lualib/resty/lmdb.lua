local _M = {}


local bit = require("bit")
local ffi = require("ffi")
local base = require("resty.core.base")
local table_new = require("table.new")
local table_clear = require("table.clear")

local lib = ffi.load("/home/datong.sun/code/dndx/lua-resty-lmdb/lmdbffi/target/debug/liblmdbffi.so")

ffi.cdef([[
typedef enum LMDBOperationCode {
  Get,
  Set,
  CreateDB,
  DropDB,
  ClearDB,
} LMDBOperationCode;

typedef enum ReturnCode {
  OK,
  ERR,
  AGAIN,
} ReturnCode;

typedef struct LMDBHandle LMDBHandle;

typedef struct LMDBResultFlags {
  uint8_t bits;
} LMDBResultFlags;
//#define LMDBResultFlags_NOT_FOUND (LMDBResultFlags){ .bits = (uint8_t)1 }
//#define LMDBResultFlags_AGAIN (LMDBResultFlags){ .bits = (uint8_t)2 }

typedef struct LMDBOperationArgs {
  const uint8_t *key;
  uintptr_t key_len;
  uint8_t *value;
  uintptr_t value_len;
  struct LMDBResultFlags flags;
} LMDBOperationArgs;

typedef struct LMDBOperation {
  enum LMDBOperationCode op_code;
  struct LMDBOperationArgs args;
} LMDBOperation;

const uint8_t *ngx_lmdb_handle_get_last_err(struct LMDBHandle *handle);

struct LMDBHandle *ngx_lmdb_handle_open(const char *path, uint32_t perm, const uint8_t **err);

void ngx_lmdb_handle_close(struct LMDBHandle *handle);

enum ReturnCode ngx_lmdb_handle_execute(struct LMDBHandle *handle,
                                        struct LMDBOperation *ops,
                                        uintptr_t n,
                                        bool write);

enum ReturnCode ngx_lmdb_handle_get_databases(struct LMDBHandle *handle,
                                              uintptr_t num_keys,
                                              uint8_t *values_buf,
                                              uintptr_t values_buf_len,
                                              int32_t *value_lens);
]])


local _MT = { __index = _M, }
local err_ptr = base.get_errmsg_ptr()
local error = error
local setmetatable = setmetatable
local assert = assert
local type = type
local band = bit.band
local C = ffi.C
local ffi_string = ffi.string
local ffi_new = ffi.new
local ffi_cast = ffi.cast
local table_new = require("table.new")
local OPS_N = 16
local CACHED_OPS = ffi.new("LMDBOperation[?]", OPS_N)
local DEFAULT_VALUE_BUF_SIZE = 1024

local LMDBResultFlags_NOT_FOUND = 1
local LMDBResultFlags_AGAIN = 2


function _M.new(path, perm)
    assert(type(path) == "string")
    assert(type(perm) == "number")

    local handle = lib.ngx_lmdb_handle_open(path, perm, ffi_cast("const unsigned char **", err_ptr))
    if handle == nil then
        return nil, ffi_string(err_ptr[0])
    end

    return setmetatable({
        handle = handle,
    }, _MT)
end


function _M:close()
    if self.handle == nil then
        error("environment already closed", 2)
    end

    lib.ngx_lmdb_handle_close(self.handle)

    self.handle = nil

    return true
end


function _M:_last_err()
    local err = lib.ngx_lmdb_handle_get_last_err(self.handle)

    return ffi.string(err)
end


local _TXN_MT = {}
_TXN_MT.__index = _TXN_MT


function _M:begin(hint)
    hint = hint or 4

    local txn = table_new(hint, 3)

    txn.n = 0
    txn.write = false
    txn.env = self

    return setmetatable(txn, _TXN_MT)
end


local function get_ops_array(len)
    if len <= OPS_N then
        return CACHED_OPS
    end

    return ffi_new("LMDBOperation[?]", len)
end


function _TXN_MT:reset()
    local env = self.env

    table_clear(self)

    self.n = 0
    self.write = false
    self.env = env
end


function _TXN_MT:get(key)
    local n = self.n + 1

    self[n] = {
        op_code = "Get",
        key = key,
    }

    self.n = n
end


function _TXN_MT:set(key, value)
    local n = self.n + 1

    self[n] = {
        op_code = "Set",
        key = key,
        value = value,
    }

    self.n = n
    self.write = true
end



function _TXN_MT:clear_db()
    local n = self.n + 1

    self[n] = {
        op_code = "ClearDB",
    }

    self.n = n
    self.write = true
end


function _TXN_MT:commit(txn)
    local ops = get_ops_array(self.n)
    local value_buf_size = DEFAULT_VALUE_BUF_SIZE

::again::
    for i = 1, self.n do
        local lop = self[i]
        local cop = ops[i - 1]

        if lop.op_code == "Get" then
            cop.op_code = C.Get
            cop.args.key = lop.key
            cop.args.key_len = #lop.key
            cop.args.value = ffi_new("uint8_t[?]", value_buf_size)
            cop.args.value_len = value_buf_size

        elseif lop.op_code == "Set" then
            cop.op_code = C.Set
            cop.args.key = lop.key
            cop.args.key_len = #lop.key
            local val = lop.value
            if val == nil then
                cop.args.value = nil
                cop.args.value_len = 0

            else
                cop.args.value = ffi_cast("uint8_t *", val)
                cop.args.value_len = #val
            end

        elseif lop.op_code == "ClearDB" then
            cop.op_code = C.ClearDB

        else
            assert(false)
        end
    end

    local ret = lib.ngx_lmdb_handle_execute(self.env.handle, ops, self.n, self.write)
    if ret == C.ERR then
        return nil, self.env:_last_err()
    end

    for i = 1, self.n do
        local cop = ops[i - 1]
        local lop = self[i]

        if cop.op_code == C.Get then
            if cop.args.flags.bits == 0 then
                lop.result = ffi_string(cop.args.value, cop.args.value_len)

            elseif band(cop.args.flags.bits, LMDBResultFlags_NOT_FOUND) then
                lop.result = nil

            elseif band(cop.args.flags.bits, LMDBResultFlags_AGAIN) then
                value_buf_size = math.max(value_buf_size, cop.args.value_len)
                goto again
            end

        elseif cop.op_code == C.Set then
            -- Set does not return flags
            lop.result = true
        end
    end

    return true
end


do
    local CACHED_TXN = _M:begin(1)

    function _M:get(key)
        CACHED_TXN.env = self

        CACHED_TXN:reset()
        CACHED_TXN:get(key)
        local res, err = CACHED_TXN:commit()
        if not res then
            return nil, err
        end

        return CACHED_TXN[1].result
    end


    function _M:set(key, value)
        CACHED_TXN.env = self

        CACHED_TXN:reset()
        CACHED_TXN:set(key, value)
        local res, err = CACHED_TXN:commit()
        if not res then
            return nil, err
        end

        return true
    end
end

return _M
