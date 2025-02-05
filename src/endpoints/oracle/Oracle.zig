const Oracle = @This();

const options = @import("options.zig");

const std = @import("std");
const ep = @import("../endpoint/endpoint.zig");
const Reader = @import("./io/Reader.zig");
const w = @import("../../wire/wire.zig");
const StringMap = std.StringHashMap([]const u8);

allocator: std.mem.Allocator,
endpoint_type: ep.EndpointType,
io: union(enum) {
    Reader: Reader,
},

pub fn init(allocator: std.mem.Allocator, endpoint_type: ep.EndpointType, options: StringMap) Oracle {
    const io = switch (options) {
        .Source => .{ .Reader = Reader.init(allocator, Sour) },
        .Sink => unreachable,
    };

    return .{
        .allocator = allocator,
        .endpoint_type = endpoint_type,
        .io = io,
    };
}

pub fn endpoint(self: *Oracle) ep.Endpoint {
    return .{
        .ptr = self,
        .vtable = &.{
            .run = run,
        },
    };
}

pub fn run(ctx: *anyopaque, wire: *w.Wire) !void {
    const self: *Oracle = @ptrCast(@alignCast(ctx));
    switch (self.io) {
        .Reader => |reader| {
            return reader.run(wire);
        },
    }
}

pub fn deinit(self: *Oracle) void {
    switch (self.io) {
        .Reader => |reader| {
            reader.deinit();
        },
    }
}
