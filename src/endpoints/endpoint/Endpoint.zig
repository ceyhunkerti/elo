const Endpoint = @This();
const std = @import("std");
const Reader = @import("Reader.zig");
const w = @import("../../wire/wire.zig");

pub const Type = enum {
    Source,
    Sink,
};

ptr: *anyopaque,
vtable: *const VTable,

pub const VTable = struct {
    run: *const fn (ctx: *anyopaque, wire: *w.Wire) anyerror!void,
};
