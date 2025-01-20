const std = @import("std");
pub const wire = @import("wire/wire.zig");
pub const Mailbox = @import("wire/Mailbox.zig");

pub const oracle = @import("endpoints/oracle/oracle.zig");
pub const postgres = @import("endpoints/postgres/postgres.zig");

pub fn main() !void {}

test {
    std.testing.refAllDecls(@This());
}
