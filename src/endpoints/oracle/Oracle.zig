const Oracle = @This();

pub const NAME = "oracle";

const std = @import("std");

const app = @import("../../app.zig");
const o = @import("options.zig");
const io = @import("io/io.zig");

pub const Source = struct {
    allocator: std.mem.Allocator,
    reader: io.Reader,

    pub fn init(allocator: std.mem.Allocator, options: o.SourceOptions) Source {
        return .{
            .allocator = allocator,
            .reader = io.Reader.init(allocator, options),
        };
    }
    pub fn deinit(self: *Source) void {
        self.reader.deinit();
    }

    pub fn source(self: *Source) anyerror!?app.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = Source.run,
                .help = Source.help,
            },
        };
    }

    pub fn run(ctx: *anyopaque, wire: *app.Wire) anyerror!void {
        const self: *Source = @ptrCast(@alignCast(ctx));
        try self.reader.run(wire);
    }

    pub fn help(ctx: *anyopaque) anyerror![]const u8 {
        const self: *Source = @ptrCast(@alignCast(ctx));
        return self.reader.help();
    }
};

pub const Sink = struct {
    allocator: std.mem.Allocator,
    writer: io.Writer,

    pub fn init(allocator: std.mem.Allocator, options: o.SinkOptions) Sink {
        return .{
            .allocator = allocator,
            .writer = io.Writer.init(allocator, options),
        };
    }
    pub fn deinit(self: *Sink) void {
        self.writer.deinit();
    }

    pub fn sink(self: *Sink) anyerror!?app.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = Sink.run,
                .help = Sink.help,
            },
        };
    }

    pub fn run(ctx: *anyopaque, wire: *app.Wire) anyerror!void {
        const self: *Sink = @ptrCast(@alignCast(ctx));
        try self.writer.run(wire);
    }

    pub fn help(ctx: *anyopaque) anyerror![]const u8 {
        const self: *Sink = @ptrCast(@alignCast(ctx));
        return self.writer.help();
    }
};

allocator: std.mem.Allocator,

_source: ?Source = null,
_sink: ?Sink = null,

pub fn init(allocator: std.mem.Allocator) Oracle {
    return .{
        .allocator = allocator,
    };
}
pub fn deinit(self: *Oracle) void {
    if (self._source) |*s| {
        s.deinit();
        self._source = null;
    }
    if (self._sink) |*s| {
        s.deinit();
        self._sink = null;
    }
}

pub fn endpoint(self: *Oracle) app.Endpoint {
    return .{
        .ptr = self,
        .vtable = &.{
            .name = NAME,
            .source = source,
            .sink = sink,
            .help = help,
        },
    };
}

pub fn help(_: *anyopaque) anyerror![]const u8 {
    return "";
}

pub fn source(ctx: *anyopaque, options: std.StringHashMap([]const u8)) anyerror!?app.Source {
    const self: *Oracle = @ptrCast(@alignCast(ctx));
    self._source = Source.init(self.allocator, try o.SourceOptions.fromStringMap(self.allocator, options));
    return self._source.?.source();
}

pub fn sink(ctx: *anyopaque, options: std.StringHashMap([]const u8)) anyerror!?app.Sink {
    const self: *Oracle = @ptrCast(@alignCast(ctx));
    self._sink = Sink.init(self.allocator, try o.SinkOptions.fromStringMap(self.allocator, options));
    return self._sink.?.sink();
}
