local _M = {}


local ffi = require("ffi")
local base = require("resty.core.base")
local table_new = require("table.new")

local lib = ffi.load("/home/datong.sun/code/dndx/lua-resty-lmdb/lmdbffi/target/debug/liblmdbffi.so")

ffi.cdef([[
typedef enum ReturnCode {
  OK,
  ERR,
  AGAIN,
} ReturnCode;

typedef struct LMDBHandle LMDBHandle;

const uint8_t *ngx_lmdb_handle_get_last_err(struct LMDBHandle *handle);

struct LMDBHandle *ngx_lmdb_handle_open(const char *path, uint32_t perm, uint8_t **err);

void ngx_lmdb_handle_close(struct LMDBHandle *handle);

enum ReturnCode ngx_lmdb_handle_get_multi(struct LMDBHandle *handle,
                                          uintptr_t num_keys,
                                          const uint8_t *const *key_ptrs,
                                          uint32_t *key_lens,
                                          uint8_t *values_buf,
                                          uintptr_t values_buf_len,
                                          int32_t *value_lens);

enum ReturnCode ngx_lmdb_handle_set_multi(struct LMDBHandle *handle,
                                          uintptr_t num_keys,
                                          const uint8_t *const *key_ptrs,
                                          uint32_t *key_lens,
                                          const uint8_t *const *value_ptrs,
                                          uint32_t *value_lens);
]])


local _MT = { __index = _M, }
local err = base.get_errmsg_ptr()
local error = error
local assert = assert
local type = type
local C = ffi.C
local ffi_string = ffi.string
local PTR_N = 16
local VALUES_BUF_SIZE = 1 * 1024 * 1024
local KEY_PTRS = ffi.new("const char *[?]", PTR_N)
local KEY_LENS = ffi.new("uint32_t [?]", PTR_N)
local VALUE_PTRS = ffi.new("const char *[?]", PTR_N)
local VALUE_LENS = ffi.new("int32_t [?]", PTR_N)
local VALUES_BUF = ffi.new("char [?]", VALUES_BUF_SIZE)
local CACHED_KEYS = {}
local CACHED_VALUES = {}


local function get_key_ptrs(n)
    if n > PTR_N then
        return ffi.new("const char *[?]", n)
    end

    return KEY_PTRS
end


local function get_key_lens(n)
    if n > PTR_N then
        return ffi.new("uint32_t [?]", n)
    end

    return KEY_LENS
end


local function get_value_ptrs(n)
    if n > PTR_N then
        return ffi.new("const char *[?]", n)
    end

    return VALUE_PTRS
end


local function get_value_lens(n)
    if n > PTR_N then
        return ffi.new("int32_t [?]", n)
    end

    return VALUE_LENS
end


function _M.new(path, perm)
    assert(type(path) == "string")
    assert(type(perm) == "number")

    local handle = lib.ngx_lmdb_handle_open(path, perm, err)
    if handle == nil then
        return nil, ffi_string(err[0])
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


function _M:get_multi(keys, nkeys, values)
    if not nkeys then
        nkeys = #keys
    end

    if not values then
        values = table_new(nkeys, 0)
    end

    local values_buf_size = VALUES_BUF_SIZE

    local key_ptrs = get_key_ptrs(nkeys)
    local key_lens = get_key_lens(nkeys)
    local values_buf = VALUES_BUF
    local value_lens = get_value_lens(nkeys)

    for i = 1, nkeys do
        local k = keys[i]
        key_ptrs[i - 1] = k
        key_lens[i - 1] = #k
    end

::again::
    local res = lib.ngx_lmdb_handle_get_multi(self.handle, nkeys, key_ptrs, key_lens, values_buf, values_buf_size, value_lens)
    if res == lib.AGAIN then
        values_buf_size = values_buf_size * 2
        values_buf = ffi.new("char [?]", values_buf_size)
        goto again
    end

    if res == lib.ERR then
        return nil, self:_last_err()
    end

    -- OK
    local offset = 0
    for i = 1, nkeys do
        if value_lens[i - 1] == -1 then
            values[i] = nil

        else
            local len = value_lens[i - 1]
            values[i] = ffi.string(values_buf + offset, len)
            offset = offset + len
        end
    end

    return values
end


function _M:get(key)
    CACHED_KEYS[1] = key

    local res, err = self:get_multi(CACHED_KEYS, 1, CACHED_VALUES)
    if not res then
        return nil, err
    end

    return res[1]
end


function _M:set(key, value)
    CACHED_KEYS[1] = key
    CACHED_VALUES[1] = value

    return self:set_multi(CACHED_KEYS, 1, CACHED_VALUES)
end


function _M:set_multi(keys, nkeys, values)
    if not nkeys then
        nkeys = #keys
    end

    if #values ~= nkeys then
        error("set_multi must have 'keys' and 'values' of the same length", 2)
    end

    local key_ptrs = get_key_ptrs(nkeys)
    local key_lens = get_key_lens(nkeys)
    local value_ptrs = get_value_ptrs(nkeys)
    local value_lens = get_value_lens(nkeys)

    for i = 1, nkeys do
        local key = keys[i]
        key_ptrs[i - 1] = key
        key_lens[i - 1] = #key

        local value = values[i]
        value_ptrs[i - 1] = value
        value_lens[i - 1] = #value
    end

    local res = lib.ngx_lmdb_handle_set_multi(self.handle, nkeys, key_ptrs, key_lens, value_ptrs, value_lens)

    if res == lib.ERR then
        return nil, self:_last_err()
    end

    assert(res == lib.OK)

    return true
end


return _M
