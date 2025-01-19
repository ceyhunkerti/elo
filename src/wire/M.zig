const std = @import("std");
const Allocator = std.mem.Allocator;

const p = @import("proto.zig");
const w = @import("wire.zig");

const Self = @This();

pub fn new(allocator: Allocator, val: anytype) !*w.Message {
    const message = try allocator.create(w.Message);
    message.* = .{
        .data = switch (@TypeOf(val)) {
            p.Metadata => .{ .Metadata = val },
            p.Record => .{ .Record = val },
            else => .Nil,
        },
    };
    return message;
}

pub fn deinit(allocator: Allocator, message: *w.Message) void {
    message.data.deinit(allocator);
    allocator.destroy(message);
}
