pub const Endpoint = @This();
pub const std = @import("std");

pub const oracle = @import("oracle/oracle.zig");
// pub const postgres = @import("postgres/postgres.zig");
// std.heap.FixedBufferAllocator

// pub const Type = enum {
//     Source,
//     Sink,
// };

// ptr: *anyopaque,
// vtable: *const VTable,

// pub const VTable = struct {
//     run: *const fn (ctx: *anyopaque, wire: *w.Wire) anyerror!void,
// };

test {
    std.testing.refAllDecls(@This());
}
