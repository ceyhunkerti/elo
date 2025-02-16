const std = @import("std");
const argz = @import("argz");
const base = @import("base");
const Command = argz.Command;
const Params = @import("commons.zig").Params;

pub const Error = error{
    OptionsRequired,
} || argz.Error || base.RegistryError;

pub fn init(allocator: std.mem.Allocator) !*Command {
    var run_cmd = Command.init(allocator, "run", struct {
        fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
            return doRun(cmd, args);
        }
    }.run);
    run_cmd.description = "Run data transfer";
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

fn doRun(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
    const params: *Params = @ptrCast(@alignCast(args));
    const options: std.ArrayList(argz.Option) = cmd.options orelse {
        return error.OptionsRequired;
    };

    const source_op = try cmd.getOption("source");
    const sink_op = try cmd.getOption("sink");

    const source_name = source_op.getString() orelse return error.OptionsRequired;
    const sink_name = sink_op.getString() orelse return error.OptionsRequired;

    // deinitialized when registry is deinitialized
    var source: base.Source = try params.endpoint_registry.getSource(source_name);
    var sink: base.Sink = try params.endpoint_registry.getSink(sink_name);

    var source_options = std.StringHashMap([]const u8).init(cmd.allocator);
    var sink_options = std.StringHashMap([]const u8).init(cmd.allocator);
    defer source_options.deinit();
    defer sink_options.deinit();

    // get source and sink options.
    // source options starts with source- and sink options starts with sink-.
    for (options.items) |option| for (option.names.items) |name| {
        if (std.mem.startsWith(u8, name, "source-")) {
            if (option.getString()) |value| {
                try source_options.put(name["source-".len..], value);
            }
        } else if (std.mem.startsWith(u8, name, "sink-")) {
            if (option.getString()) |value| {
                try sink_options.put(name["sink-".len..], value);
            }
        }
    };

    try source.prepare(source_options);
    try sink.prepare(sink_options);

    var wire = base.Wire.init();

    const producer = struct {
        pub fn producerRunner(s: *base.Source, w: *base.Wire) !void {
            try s.run(w);
        }
    };
    const consumer = struct {
        pub fn consumerRunner(s: *base.Sink, w: *base.Wire) !void {
            try s.run(w);
        }
    };

    var pth = try std.Thread.spawn(.{ .allocator = cmd.allocator }, producer.producerRunner, .{ &source, &wire });
    var cth = try std.Thread.spawn(.{ .allocator = cmd.allocator }, consumer.consumerRunner, .{ &sink, &wire });

    pth.join();
    cth.join();

    if (wire.err) |err| {
        std.debug.print("Data transfer failed\n", .{});
        std.debug.print("Error: {any}\n", .{err});
        return 1;
    }

    return 0;
}
