const std = @import("std");
const c = @import("c.zig").c;
const Privilege = @import("./Connection.zig").Privilege;

pub const ConnectionOptions = struct {
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
    privilege: Privilege = .DEFAULT,
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u64 = 10_000,
    sql: []const u8,

    // pub fn validate() !void {} // TODO
};

pub const SinkOptions = struct {
    connection: ConnectionOptions,
    table: ?[]const u8,
    sql: ?[]const u8 = null,
    mode: enum { Append, Truncate, Create } = .Append,
    create_sql: ?[]const u8 = null,
    batch_size: u64 = 10_000,

    // pub fn validate() !void {} // TODO
};

pub const Options = union(enum) {
    Source: SourceOptions,
    Sink: SinkOptions,
};
