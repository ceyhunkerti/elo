const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;
const Argument = argz.Argument;
const BaseSource = base.Source;
const BaseSink = base.Sink;
const EndpointRegistry = base.EndpointRegistry;

pub const Params = struct {
    endpoint_registry: *EndpointRegistry,
};

pub fn init(allocator: std.mem.Allocator) !Command {
    var root = Command.init(allocator, "elo", null);
    var list_cmd = try initList(allocator);
    try root.addCommand(&list_cmd);
    var info_cmd = try initInfo(allocator);
    try root.addCommand(&info_cmd);
    var run_cmd = try initRun(allocator);
    try root.addCommand(&run_cmd);

    return root;
}

fn initList(allocator: std.mem.Allocator) !Command {
    var list = Command.init(allocator, "list", null);

    var list_source_endpoints = Command.init(allocator, "source-endpoints", struct {
        fn run(_: *const Command, args: ?*anyopaque) anyerror!i32 {
            const params: *Params = @ptrCast(@alignCast(args));
            var it = params.endpoint_registry.sources.keyIterator();
            std.debug.print("Source Endpoints:\n", .{});
            while (it.next()) |name| {
                std.debug.print("- {s}\n", .{name.*});
            }
            return 0;
        }
    }.run);
    try list.addCommand(&list_source_endpoints);

    return list;
}

fn initInfo(allocator: std.mem.Allocator) !Command {
    var info = Command.init(allocator, "info", null);
    var source = Command.init(allocator, "source", struct {
        fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
            const params: *Params = @ptrCast(@alignCast(args));
            if (cmd.arguments) |arguments| {
                const name = try arguments.items[0].getString() orelse {
                    std.debug.print("Missing required argument SOURCE_NAME\n", .{});
                    return error.Fail;
                };

                const source: BaseSource = params.endpoint_registry.sources.get(name) orelse {
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
        "name",
        Argument.Type.String,
        "Source name",
        null,
        true,
    ));
    try info.addCommand(&source);

    var sink = Command.init(allocator, "sink", struct {
        fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
            const params: *Params = @ptrCast(@alignCast(args));
            if (cmd.arguments) |arguments| {
                const name = try arguments.items[0].getString() orelse {
                    std.debug.print("Missing required argument SINK_NAME\n", .{});
                    return error.Fail;
                };

                const sink: BaseSink = params.endpoint_registry.sinks.get(name) orelse {
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
        "name",
        Argument.Type.String,
        "Sink name",
        null,
        true,
    ));
    try info.addCommand(&sink);

    return info;
}

fn initRun(allocator: std.mem.Allocator) !Command {
    const runFn = @import("run.zig").run;

    var run_cmd = Command.init(allocator, "run", struct {
        fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
            return runFn(cmd, args);
        }
    }.run);
    run_cmd.allow_unknown_options = true;

    try run_cmd.addOption(try argz.Option.init(
        allocator,
        .String,
        &.{"source"},
        "source endpoint name",
    ));

    try run_cmd.addOption(try argz.Option.init(
        allocator,
        .String,
        &.{"sink"},
        "sink endpoint name",
    ));

    return run_cmd;
}
