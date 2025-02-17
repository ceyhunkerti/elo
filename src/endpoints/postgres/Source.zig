const Source = @This();

const std = @import("std");

const base = @import("base");
const Wire = base.Wire;
const BaseSource = base.Source;
const io = @import("io/io.zig");
const opts = @import("options.zig");
const SourceOptions = opts.SourceOptions;
const Connection = @import("Connection.zig");
const QueryMetadata = base.QueryMetadata;
const metadata = @import("metadata/metadata.zig");

const constants = @import("constants.zig");

pub const Error = error{
    NotInitialized,
    OptionsRequired,
};

allocator: std.mem.Allocator,
options: ?SourceOptions = null,
readers: ?[]io.Reader = null,

pub fn init(allocator: std.mem.Allocator) Source {
    return .{ .allocator = allocator };
}

pub fn deinit(ctx: *anyopaque) void {
    var self: *Source = @ptrCast(@alignCast(ctx));
    if (self.options) |*options| options.deinit(self.allocator);
    if (self.readers) |readers| {
        for (readers) |reader| {
            reader.deinit();
        }
        self.allocator.free(readers);
    }
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

        self.readers = self.allocator.alloc(io.Reader, self.options.?.parallel);

        for (0..self.options.?.parallel) |i| {
            const conn = try self.allocator.create(Connection);
            conn.* = Connection.init(
                self.allocator,
                self.options.?.connection.username,
                self.options.?.connection.password,
                self.options.?.connection.host,
                self.options.?.connection.database,
            );
            const reader = io.Reader.init(
                self.allocator,
                @intCast(i + 1),
                conn,
                self.options.?.sql,
                self.options.?.fetch_size,
            );
            self.readers[i] = reader;
        }
    } else {
        return Error.OptionsRequired;
    }
}

pub fn run(ctx: *anyopaque, wire: *Wire) anyerror!void {
    const self: *Source = @ptrCast(@alignCast(ctx));

    if (self.readers) |readers| {
        for (readers) |reader| {
            try reader.run(wire);
        }
    } else {
        return Error.NotInitialized;
    }
}

pub fn info(ctx: *anyopaque) anyerror![]const u8 {
    const self: *Source = @ptrCast(@alignCast(ctx));
    return try io.Reader.info(self.allocator);
}

fn findQueryMetadata(self: *Source) !metadata.QueryMetadata {
    var conn = Connection.init(
        self.allocator,
        self.options.?.connection.username,
        self.options.?.connection.password,
        self.options.?.connection.host,
        self.options.?.connection.database,
    );
    defer conn.deinit();
    try conn.connect();

    return try metadata.findQueryMetadata(self.allocator, &conn, self.options.?.sql);
}
