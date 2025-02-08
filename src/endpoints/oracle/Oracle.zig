const Oracle = @This();

const std = @import("std");
const ep = @import("../endpoint/endpoint.zig");
const w = @import("../../wire/wire.zig");
const StringMap = std.StringHashMap([]const u8);
const opts = @import("options.zig");
const io = @import("io/io.zig");

pub const NAME = "oracle";

const Source = struct {
    allocator: std.mem.Allocator,
    reader: io.Reader,

    pub fn init(allocator: std.mem.Allocator, options: opts.SourceOptions) Source {
        return .{
            .allocator = allocator,
            .reader = io.Reader.init(allocator, options),
        };
    }
    pub fn deinit(self: *Source) void {
        self.reader.deinit();
    }

    pub fn source(self: *Source) anyerror!?ep.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = Source.run,
                .help = Source.help,
            },
        };
    }

    pub fn run(ctx: *anyopaque, wire: *w.Wire) anyerror!void {
        const self: *Source = @ptrCast(@alignCast(ctx));
        try self.reader.run(wire);
    }

    pub fn help(ctx: *anyopaque) anyerror![]const u8 {
        const self: *Source = @ptrCast(@alignCast(ctx));
        return self.reader.help();
    }
};

allocator: std.mem.Allocator,

// kept to manage resources
_source: ?Source = null,

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
}

pub fn endpoint(self: *Oracle) ep.Endpoint {
    return .{
        .ptr = self,
        .vtable = &.{
            .name = NAME,
            .source = source,
            .sink = null,
            .help = help,
        },
    };
}

pub fn help(_: *anyopaque) anyerror![]const u8 {
    return "";
}

pub fn source(ctx: *anyopaque, options: StringMap) anyerror!?ep.Source {
    const self: *Oracle = @ptrCast(@alignCast(ctx));
    self._source = Source.init(self.allocator, try opts.SourceOptions.fromStringMap(self.allocator, options));
    return self._source.?.source();
}
