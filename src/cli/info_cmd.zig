const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;
const BaseSource = base.Source;
const BaseSink = base.Sink;
const EndpointRegistry = base.EndpointRegistry;
const Context = @import("context.zig").Context;
const Argument = argz.Argument;

pub fn init(allocator: std.mem.Allocator) !*Command {
    var info = Command.init(allocator, "info", null);
    info.description = "Endpoint options info";

    const source = Command.init(allocator, "source", struct {
        fn run(cmd: *const Command, ctx: ?*anyopaque) anyerror!i32 {
            const context: *Context = @ptrCast(@alignCast(ctx));
            if (cmd.arguments) |arguments| {
                const name = try arguments.items[0].getString() orelse {
                    std.debug.print("Missing required argument SOURCE_NAME\n", .{});
                    return error.Fail;
                };

                const source: BaseSource = context.endpoint_registry.sources.get(name) orelse {
                    std.debug.print("Unknown source endpoint: [{s}]\n", .{name});
                    return error.Fail;
                };
                const source_info = try source.info();
                defer source.allocator.free(source_info);
                std.debug.print("Source info for [{s}]:\n", .{name});
                std.debug.print("{s}\n", .{source_info});
            } else {
                std.debug.print("Missing required argument SOURCE_NAME\n", .{});
                return error.Fail;
            }
            return 0;
        }
    }.run);
    try source.addArgument(try Argument.init(
        allocator,
        "NAME",
        Argument.Type.String,
        "Source name",
        null,
        true,
    ));
    try info.addCommand(source);

    const sink = Command.init(allocator, "sink", struct {
        fn run(cmd: *const Command, ctx: ?*anyopaque) anyerror!i32 {
            const context: *Context = @ptrCast(@alignCast(ctx));
            if (cmd.arguments) |arguments| {
                const name = try arguments.items[0].getString() orelse {
                    std.debug.print("Missing required argument SINK_NAME\n", .{});
                    return error.Fail;
                };

                const sink: BaseSink = context.endpoint_registry.sinks.get(name) orelse {
                    std.debug.print("Unknown sink endpoint: [{s}]\n", .{name});
                    return error.Fail;
                };
                const sink_info = try sink.info();
                defer sink.allocator.free(sink_info);
                std.debug.print("Sink info for [{s}]:\n", .{name});
                std.debug.print("{s}\n", .{sink_info});
            } else {
                std.debug.print("Missing required argument SOURCE_NAME\n", .{});
                return error.Fail;
            }
            return 0;
        }
    }.run);
    try sink.addArgument(try Argument.init(
        allocator,
        "NAME",
        Argument.Type.String,
        "Sink name",
        null,
        true,
    ));
    try info.addCommand(sink);

    return info;
}
