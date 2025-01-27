const std = @import("std");
const c = @import("c.zig").c;

pub const ConnectionOptions = struct {
    host: []const u8,
    database: []const u8,
    username: []const u8,
    password: []const u8,
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u32 = 10_000,
    sql: []const u8,
};

pub const SinkOptions = struct {
    allocator: std.mem.Allocator = undefined,
    connection: ConnectionOptions,
    table: []const u8,
    columns: ?[][]const u8 = null,
    sql: ?[]const u8 = null,
    mode: enum { Append, Truncate } = .Append,
    batch_size: u32 = 10_000,
};
