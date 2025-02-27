const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;
const Context = @import("context.zig").Context;

pub fn init(allocator: std.mem.Allocator) !*Command {
    var list = Command.init(allocator, "list", null);
    list.description = "List endpoints";

    const sources = Command.init(allocator, "sources", struct {
        fn run(_: *const Command, ctx: ?*anyopaque) anyerror!i32 {
            const context: *Context = @ptrCast(@alignCast(ctx));
            var it = context.endpoint_registry.sources.keyIterator();
            std.debug.print("Source Endpoints:\n", .{});
            while (it.next()) |name| {
                std.debug.print("- {s}\n", .{name.*});
            }
            return 0;
        }
    }.run);
    const sinks = Command.init(allocator, "sinks", struct {
        fn run(_: *const Command, ctx: ?*anyopaque) anyerror!i32 {
            const context: *Context = @ptrCast(@alignCast(ctx));
            var it = context.endpoint_registry.sinks.keyIterator();
            std.debug.print("Sink Endpoints:\n", .{});
            while (it.next()) |name| {
                std.debug.print("- {s}\n", .{name.*});
            }
            return 0;
        }
    }.run);

    try list.addCommand(sources);
    try list.addCommand(sinks);

    return list;
}
