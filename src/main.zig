const std = @import("std");
pub const oracle = @import("endpoints/oracle/oracle.zig");
pub const AtomicBlockingQueue = @import("queue.zig").AtomicBlockingQueue;
pub const commons = @import("commons.zig");

const e = union(enum) {
    A: ?bool,
    B: ?i8,
};

pub fn main() !void {}

test {
    std.testing.refAllDecls(@This());
}
