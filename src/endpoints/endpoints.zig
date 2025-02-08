pub const Endpoint = @This();
pub const std = @import("std");

pub const oracle = @import("oracle/oracle.zig");
pub const postgres = @import("postgres/postgres.zig");

test {
    std.testing.refAllDecls(@This());
}
