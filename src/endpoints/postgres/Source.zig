const Source = @This();

const std = @import("std");
const base = @import("base");
const io = @import("io/io.zig");
const opts = @import("options.zig");
const Connection = @import("Connection.zig");
const metadata = @import("metadata/metadata.zig");

const log = std.log;
const Allocator = std.mem.Allocator;

const Wire = base.Wire;
const BaseSource = base.Source;
const helpers = base.helpers;
const QueryMetadata = base.QueryMetadata;

const SourceOptions = opts.SourceOptions;

const constants = @import("constants.zig");

pub const Error = error{
    NotInitialized,
    OptionsRequired,
};

allocator: Allocator,
options: ?SourceOptions = null,
readers: ?[]io.Reader = null,

pub fn init(allocator: Allocator) Source {
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
    if (options == null) return Error.OptionsRequired;

    self.options = try SourceOptions.fromMap(self.allocator, options.?);

    const qmd = try self.findQueryMetadata();
    defer qmd.deinit();

    const chunks = try qmd.chunks(self.options.?.parallel);

    if (chunks.len < self.options.?.parallel) {
        log.warn("Not enough rows for parallel execution. Using {d} threads", .{chunks.len});
    }

    self.readers = self.allocator.alloc(io.Reader, chunks.len);

    for (chunks) |i| {
        const conn = try self.allocator.create(Connection);
        conn.* = self.options.?.connection.toConnection();
        const reader = io.Reader.init(
            self.allocator,
            @intCast(i + 1),
            conn,
            self.options.?.sql,
            self.options.?.fetch_size,
        );
        self.readers[i] = reader;
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
    var conn = self.options.?.connection.toConnection();
    defer conn.deinit();
    try conn.connect();

    return try metadata.findQueryMetadata(self.allocator, &conn, self.options.?.sql);
}
