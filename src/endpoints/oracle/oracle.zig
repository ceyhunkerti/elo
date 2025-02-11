const std = @import("std");
const Source = @import("Source.zig");
const Sink = @import("Sink.zig");
const base = @import("base");

pub fn source(allocator: std.mem.Allocator) base.Source {
    const s = allocator.create(Source) catch unreachable;
    s.* = Source.init(allocator);
    return s.get();
}

pub fn sink(allocator: std.mem.Allocator) base.Sink {
    const s = allocator.create(Sink) catch unreachable;
    s.* = Sink.init(allocator);
    return s.get();
}

test "source" {
    const allocator = std.testing.allocator;
    var s = source(allocator);
    defer s.deinit();
    const h = try s.help();
    defer s.allocator.free(h);
    try std.testing.expectEqualStrings("hello from reader", h);
}

test {
    std.testing.refAllDecls(@This());
}
