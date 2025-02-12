const Source = @This();

const std = @import("std");

const base = @import("base");
const Wire = base.Wire;
const BaseSource = base.Source;
const io = @import("io/io.zig");
const opts = @import("options.zig");
const SourceOptions = opts.SourceOptions;

const constants = @import("constants.zig");

pub const Error = error{
    NotInitialized,
    OptionsRequired,
};

allocator: std.mem.Allocator,
reader: ?io.Reader = null,
options: ?SourceOptions = null,

pub fn init(allocator: std.mem.Allocator) Source {
    return .{ .allocator = allocator };
}
pub fn deinit(ctx: *anyopaque) void {
    var self: *Source = @ptrCast(@alignCast(ctx));
    if (self.reader) |*reader| reader.deinit();
    if (self.options) |*options| options.deinit(self.allocator);
    self.allocator.destroy(self);
}

pub fn get(self: *Source) BaseSource {
    return .{
        .ptr = self,
        .name = constants.NAME,
        .allocator = self.allocator,
        .vtable = &.{
            .prepare = prepare,
            .run = run,
            .info = info,
            .deinit = deinit,
        },
    };
}

pub fn prepare(ctx: *anyopaque, options: ?std.StringHashMap([]const u8)) anyerror!void {
    const self: *Source = @ptrCast(@alignCast(ctx));
    if (options) |o| {
        self.options = try SourceOptions.fromMap(self.allocator, o);
        self.reader = io.Reader.init(self.allocator, self.options.?);
    } else {
        return Error.OptionsRequired;
    }
}

pub fn run(ctx: *anyopaque, wire: *Wire) anyerror!void {
    const self: *Source = @ptrCast(@alignCast(ctx));
    return if (self.reader) |*reader| try reader.run(wire) else error.NotInitialized;
}

pub fn info(ctx: *anyopaque) anyerror![]const u8 {
    const self: *Source = @ptrCast(@alignCast(ctx));
    return try io.Reader.info(self.allocator);
}
