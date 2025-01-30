const std = @import("std");
const c = @import("c.zig").c;
const Options = @import("Copy.zig").Options;

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
    columns: ?[][][:0]const u8 = null,
    table: [:0]const u8,
    mode: enum { Append, Truncate } = .Append,
    copy_options: ?Options = null,
};
