const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;

const EndpointRegistry = base.EndpointRegistry;

pub const Params = struct {
    registry: *EndpointRegistry,
};

pub fn init(allocator: std.mem.Allocator) !*Command {
    const root = Command.init(allocator, "elo", null);

    const list = Command.init(allocator, "list", null);

    const endpoints = Command.init(allocator, "source-endpoints", struct {
        fn run(_: *const Command, args: ?*anyopaque) anyerror!i32 {
            const params: *Params = @ptrCast(@alignCast(args));
            var it = params.registry.sources.keyIterator();
            std.debug.print("Source Endpoints:\n", .{});
            while (it.next()) |name| {
                std.debug.print("- {s}\n", .{name.*});
            }
            return 0;
        }
    }.run);
    try root.addCommand(list);
    try list.addCommand(endpoints);

    return root;
}
