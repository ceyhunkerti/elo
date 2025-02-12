const std = @import("std");
const Source = @import("Source.zig");
const Sink = @import("Sink.zig");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;
const constants = @import("constants.zig");

pub fn source(allocator: std.mem.Allocator) !base.Source {
    const s = try allocator.create(Source);
    s.* = Source.init(allocator);
    return s.get();
}

pub fn sink(allocator: std.mem.Allocator) !base.Sink {
    const s = try allocator.create(Sink);
    s.* = Sink.init(allocator);
    return s.get();
}

pub fn register(registry: *EndpointRegistry) !void {
    try registry.sources.put(constants.NAME, try source(registry.allocator));
    try registry.sinks.put(constants.NAME, try sink(registry.allocator));
}

test "source" {
    const allocator = std.testing.allocator;
    var s = try source(allocator);
    defer s.deinit();
    const h = try s.info();
    defer s.allocator.free(h);
    try std.testing.expectEqualStrings("hello from reader", h);
}

test {
    std.testing.refAllDecls(@This());
}
