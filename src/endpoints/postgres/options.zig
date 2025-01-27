const std = @import("std");
const c = @import("c.zig").c;

pub const ConnectionOptions = struct {
    host: [:0]const u8,
    database: [:0]const u8,
    username: [:0]const u8,
    password: [:0]const u8,
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u32 = 10_000,
    sql: [:0]const u8,
};

pub const SinkOptions = struct {
    allocator: std.mem.Allocator = undefined,
    connection: ConnectionOptions,
    table: [:0]const u8,
    columns: ?[][][:0]const u8 = null,
    sql: ?[:0]const u8 = null,
    mode: enum { Append, Truncate } = .Append,
    batch_size: u32 = 10_000,
};
