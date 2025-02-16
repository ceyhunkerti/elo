const std = @import("std");
const cli = @import("cli/cli.zig");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;
const CliContext = @import("cli/context.zig").Context;
const Allocator = std.mem.Allocator;

pub const std_options: std.Options = .{
    .logFn = logFn,
    .log_level = .debug,
};

var log_level = std.log.default_level;

fn logFn(
    comptime message_level: std.log.Level,
    comptime scope: @TypeOf(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    if (@intFromEnum(message_level) <= @intFromEnum(log_level)) {
        std.log.defaultLog(message_level, scope, format, args);
    }
}

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
        std.log.err("Run failed: {s}\n", .{@errorName(err)});
        return;
    };
}

fn run(allocator: Allocator, registry: *EndpointRegistry) !i32 {
    var cmd = try cli.init(allocator);
    defer cmd.deinit();

    var params = CliContext{ .endpoint_registry = registry, .log_level = &log_level };
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
