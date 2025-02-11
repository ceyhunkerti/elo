const Sink = @This();
const std = @import("std");
const w = @import("../wire/wire.zig");

allocator: std.mem.Allocator,
name: []const u8,
ptr: *anyopaque,
vtable: *const VTable,

pub const VTable = struct {
    prepare: *const fn (ctx: *anyopaque, options: ?std.StringHashMap([]const u8)) anyerror!void,
    run: *const fn (ctx: *anyopaque, wire: *w.Wire) anyerror!void,
    help: *const fn (ctx: *anyopaque) anyerror![]const u8,
    deinit: *const fn (ctx: *anyopaque) void,
};

pub fn help(self: Sink) anyerror![]const u8 {
    return self.vtable.help(self.ptr);
}

pub fn deinit(self: Sink) void {
    self.vtable.deinit(self.ptr);
}
