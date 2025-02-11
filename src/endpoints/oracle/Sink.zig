const Sink = @This();

const std = @import("std");

const base = @import("base");
const Wire = base.Wire;
const BaseSink = base.Sink;
const io = @import("io/io.zig");
const opts = @import("options.zig");
const SinkOptions = opts.SinkOptions;

pub const Error = error{
    NotInitialized,
    OptionsRequired,
};

allocator: std.mem.Allocator,
writer: ?io.Writer = null,

pub fn init(allocator: std.mem.Allocator) Sink {
    return .{ .allocator = allocator };
}
pub fn deinit(ctx: *anyopaque) void {
    var self: *Sink = @ptrCast(@alignCast(ctx));
    if (self.writer) |*writer| writer.deinit();
    self.allocator.destroy(self);
}

pub fn get(self: *Sink) BaseSink {
    return .{
        .ptr = self,
        .allocator = self.allocator,
        .vtable = &.{
            .prepare = prepare,
            .run = run,
            .help = help,
            .deinit = deinit,
        },
    };
}

pub fn prepare(ctx: *anyopaque, options: ?std.StringHashMap([]const u8)) anyerror!void {
    const self: *Sink = @ptrCast(@alignCast(ctx));
    if (options) |o| {
        self.writer = io.Writer.init(
            self.allocator,
            try SinkOptions.fromStringMap(self.allocator, o),
        );
    } else {
        return Error.OptionsRequired;
    }
}

pub fn run(ctx: *anyopaque, wire: *Wire) anyerror!void {
    const self: *Sink = @ptrCast(@alignCast(ctx));
    return if (self.writer) |*writer| try writer.run(wire) else error.NotInitialized;
}

pub fn help(ctx: *anyopaque) anyerror![]const u8 {
    const self: *Sink = @ptrCast(@alignCast(ctx));
    return io.Writer.help(self.allocator);
}
