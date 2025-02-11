const std = @import("std");
const cli = @import("cli.zig");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) @panic("memory leaked");
    }
    const allocator = gpa.allocator();

    var registry = EndpointRegistry.init(allocator);
    try register(&registry);

    defer registry.deinit();

    try cli.init(allocator, &registry);
}

pub fn register(registry: *EndpointRegistry) !void {
    const oracle = @import("endpoints/oracle/oracle.zig");

    try oracle.register(registry);
}

test {
    std.testing.refAllDecls(@This());
    _ = @import("base");
}
