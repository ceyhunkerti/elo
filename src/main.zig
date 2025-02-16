const std = @import("std");
const cli = @import("cli/cli.zig");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;
const CliParams = @import("cli/commons.zig").Params;
const Allocator = std.mem.Allocator;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) @panic("memory leaked");
    }
    const allocator = gpa.allocator();

    var registry = EndpointRegistry.init(allocator);
    defer registry.deinit();

    try registerEndpoints(&registry);

    _ = run(allocator, &registry) catch |err| {
        std.debug.print("run failed: {s}\n", .{@errorName(err)});
        return;
    };
}

fn run(allocator: Allocator, registry: *EndpointRegistry) !i32 {
    var cmd = try cli.init(allocator);
    defer cmd.deinit();

    var params = CliParams{ .endpoint_registry = registry };
    try cmd.parse();

    return try cmd.run(&params);
}

fn registerEndpoints(registry: *EndpointRegistry) !void {
    const oracle = @import("endpoints/oracle/oracle.zig");
    const postgres = @import("endpoints/postgres/postgres.zig");

    try oracle.register(registry);
    try postgres.register(registry);
}

test {
    std.testing.refAllDecls(@This());
}
