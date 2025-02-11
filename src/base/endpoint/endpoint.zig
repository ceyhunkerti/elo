const std = @import("std");
pub const Source = @import("Source.zig");
pub const Sink = @import("Sink.zig");

pub const Registry = struct {
    allocator: std.mem.Allocator,
    sources: std.StringHashMap(Source),
    sinks: std.StringHashMap(Sink),
    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{
            .allocator = allocator,
            .sources = std.StringHashMap(Source).init(allocator),
            .sinks = std.StringHashMap(Sink).init(allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        self.sources.deinit();
        self.sinks.deinit();
    }
};
