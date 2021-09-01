# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * blocks() * 5;

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lualib/?.lua;;";

    init_worker_by_lua_block {
        os.execute("mkdir /tmp/test-lua-resty-lmdb")
        local lmdb = require("resty.lmdb")
        _G.l = assert(lmdb.new("/tmp/test-lua-resty-lmdb", tonumber("600", 8)))
    }
};

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: simple set() / get()
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            ngx.say(l:set("test", "value"))
            ngx.say(l:get("test"))
        }
    }
--- request
GET /t
--- response_body
true
value
--- no_error_log
[error]
[warn]
[crit]



=== TEST 2: clear using set()
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            ngx.say(l:set("test", "value"))
            ngx.say(l:set("test", nil))
            ngx.say(l:get("test"))
        }
    }
--- request
GET /t
--- response_body
true
true
nil
--- no_error_log
[error]
[warn]
[crit]
