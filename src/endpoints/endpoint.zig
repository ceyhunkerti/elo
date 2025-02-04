pub const std = @import("std");

pub const oracle = @import("oracle/oracle.zig");
pub const postgres = @import("postgres/postgres.zig");

pub const EndpointType = enum {
    Source,
    Sink,
};

test {
    std.testing.refAllDecls(@This());
}
