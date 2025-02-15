const std = @import("std");
const base = @import("base");
const argz = @import("argz");
const Command = argz.Command;
const Params = @import("cli.zig").Params;
const EndpointRegistry = base.EndpointRegistry;

pub const Error = error{
    OptionsRequired,
} || argz.Error || base.RegistryError;

fn getOptionValueMap(
    allocator: std.mem.Allocator,
    prefix: []const u8,
    options: std.ArrayList(argz.Option),
) !std.StringHashMap([]const u8) {
    var map = std.StringHashMap([]const u8).init(allocator);
    for (options.items) |option| {
        for (option.names.items) |name| {
            if (std.mem.startsWith(u8, name, prefix)) {
                if (option.getString()) |value| {
                    try map.put(name[prefix.len..], value);
                }
            }
        }
    }
    return map;
}

pub fn run(cmd: *const Command, args: ?*anyopaque) anyerror!i32 {
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

    var source_options = try getOptionValueMap(cmd.allocator, "source-", options);
    defer source_options.deinit();
    var sink_options = try getOptionValueMap(cmd.allocator, "sink-", options);
    defer sink_options.deinit();

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
