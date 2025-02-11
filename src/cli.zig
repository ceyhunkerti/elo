const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;

const EndpointRegistry = base.EndpointRegistry;

pub fn init(allocator: std.mem.Allocator, registry: *EndpointRegistry) !void {
    var root = argz.Command.init(allocator, "elo", null);
    defer root.deinit();
    var list = argz.Command.init(allocator, "list", null);

    const Ctx = struct {
        registry: *EndpointRegistry,
    };
    var ctx = Ctx{ .registry = registry };

    var endpoints = argz.Command.init(allocator, "source-endpoints", struct {
        fn run(_: *const Command, args: ?*anyopaque) anyerror!i32 {
            const a: *Ctx = @ptrCast(@alignCast(args));
            var it = a.registry.sources.keyIterator();
            while (it.next()) |name| {
                std.debug.print("{s}\n", .{name});
            }
            return 0;
        }
    }.run);
    try root.addCommand(&list);
    try list.addCommand(&endpoints);

    _ = try root.run(&ctx);
}
