const Writer = @This();
const std = @import("std");
const w = @import("../../wire/wire.zig");

ptr: *anyopaque,
vtable: *const VTable,

pub const VTable = struct {
    run: *const fn (ctx: *anyopaque, wire: *w.Wire) anyerror!void,
    help: *const fn (ctx: *anyopaque) anyerror![]const u8,
};
