const Endpoint = @This();
const std = @import("std");
const w = @import("../../wire/wire.zig");
const StringMap = std.StringHashMap([]const u8);
const Sink = @import("Sink.zig");
const Source = @import("Source.zig");

// example
// e = Oracle.init(allocator).source(options);
// e = Oracle.init(allocator).sink(options);

ptr: *anyopaque,
vtable: *const VTable,

pub const VTable = struct {
    name: []const u8,
    help: *const fn (ctx: *anyopaque) anyerror![]const u8,
    source: ?*const fn (ctx: *anyopaque, options: StringMap) anyerror!?Source,
    sink: ?*const fn (ctx: *anyopaque, options: StringMap) anyerror!?Sink,
};

pub fn source(self: *Endpoint, options: StringMap) anyerror!?Source {
    if (self.vtable.source) |source_| {
        return source_(self.ptr, options);
    }
    return null;
}

pub fn sink(self: *Endpoint, options: StringMap) anyerror!?Sink {
    if (self.vtable.sink) |sink_| {
        return sink_(self.ptr, options);
    }
    return null;
}
