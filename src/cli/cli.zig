const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;
const BaseSource = base.Source;
const BaseSink = base.Sink;
const EndpointRegistry = base.EndpointRegistry;

pub const Params = struct {
    endpoint_registry: *EndpointRegistry,
};

pub fn init(allocator: std.mem.Allocator) !*Command {
    const root = Command.init(allocator, "elo", null);
    try root.addCommand(try initList(allocator));
    try root.addCommand(try initInfo(allocator));
    try root.addCommand(try initRun(allocator));

    return root;
}

fn initList(allocator: std.mem.Allocator) !*Command {
    const list = Command.init(allocator, "list", null);

    const list_source_endpoints = Command.init(allocator, "source-endpoints", struct {
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
    try list.addCommand(list_source_endpoints);

    return list;
}

fn initInfo(allocator: std.mem.Allocator) !*Command {
    const info = Command.init(allocator, "info", null);
    const source = Command.init(allocator, "source", struct {
        fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
            const params: *Params = @ptrCast(@alignCast(args));
            if (cmd.arguments) |arguments| {
                if (arguments.values()) |values| {
                    const name = values[0].String;
                    const source: BaseSource = params.endpoint_registry.sources.get(name) orelse {
                        std.debug.print("Unknown source endpoint: [{s}]\n", .{name});
                        return error.Fail;
                    };
                    const source_info = try source.info();
                    defer source.allocator.free(source_info);
                    std.debug.print("Source info for [{s}]:\n", .{name});
                    std.debug.print("{s}\n", .{source_info});
                }
            } else {
                std.debug.print("Missing required argument SOURCE_NAME\n", .{});
                return error.Fail;
            }
            return 0;
        }
    }.run);
    source.arguments = .{ .count = 1 };
    try info.addCommand(source);

    const sink = Command.init(allocator, "sink", struct {
        fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
            const params: *Params = @ptrCast(@alignCast(args));
            if (cmd.arguments) |arguments| {
                if (arguments.values()) |values| {
                    const name = values[0].String;
                    const sink: BaseSink = params.endpoint_registry.sinks.get(name) orelse {
                        std.debug.print("Unknown sink endpoint: [{s}]\n", .{name});
                        return error.Fail;
                    };
                    const sink_info = try sink.info();
                    defer sink.allocator.free(sink_info);
                    std.debug.print("Sink info for [{s}]:\n", .{name});
                    std.debug.print("{s}\n", .{sink_info});
                }
            } else {
                std.debug.print("Missing required argument SINK_NAME\n", .{});
                return error.Fail;
            }
            return 0;
        }
    }.run);
    sink.arguments = .{ .count = 1 };
    try info.addCommand(sink);

    return info;
}

fn initRun(allocator: std.mem.Allocator) !*Command {
    const runFn = @import("run.zig").run;

    const run_cmd = Command.init(allocator, "run", struct {
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
