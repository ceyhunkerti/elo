const std = @import("std");

const Self = @This();

allocator: std.mem.Allocator,

owner: []const u8,
name: []const u8,

pub fn init(allocator: std.mem.Allocator, owner: []const u8, name: []const u8) Self {
    return .{
        .allocator = allocator,
        .owner = owner,
        .name = name,
    };
}
