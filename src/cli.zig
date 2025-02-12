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
    try root.addCommand(try initList(allocator));

    return root;
}

fn initList(allocator: std.mem.Allocator) !*Command {
    const list = Command.init(allocator, "list", null);

    const list_source_endpoints = Command.init(allocator, "source-endpoints", struct {
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
    try list.addCommand(list_source_endpoints);

    return list;
}
