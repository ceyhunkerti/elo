const std = @import("std");
const cli = @import("cli/cli.zig");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;
const CliParams = @import("cli/commons.zig").Params;

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

    var cmd = try cli.init(allocator);
    defer cmd.deinit();

    var params = CliParams{ .endpoint_registry = &registry };
    try cmd.parse();

    _ = try cmd.run(&params);
}

pub fn register(registry: *EndpointRegistry) !void {
    const oracle = @import("endpoints/oracle/oracle.zig");
    const postgres = @import("endpoints/postgres/postgres.zig");

    try oracle.register(registry);
    try postgres.register(registry);
}

test {
    std.testing.refAllDecls(@This());
}
