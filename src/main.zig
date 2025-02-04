const std = @import("std");
pub const wire = @import("wire/wire.zig");
pub const proto = @import("wire/proto/proto.zig");
pub const endpoints = @import("endpoints/endpoint.zig");

pub fn main() !void {}

test {
    std.testing.refAllDecls(@This());
}

test {
    _ = @import("endpoints/tests/test_pg_ora.zig");
}
