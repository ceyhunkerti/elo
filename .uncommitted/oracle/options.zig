const std = @import("std");
const c = @import("c.zig").c;

pub const ConnectionOptions = struct {
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
    role: []const u8,

    pub fn authMode(self: ConnectionOptions) c_int {
        if (std.mem.eql(u8, self.role, "SYSDBA")) {
            return c.DPI_MODE_AUTH_SYSDBA;
        }
        return c.DPI_MODE_AUTH_DEFAULT;
    }
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u64 = 10_000,
    sql: []const u8,
};

pub const SinkOptions = struct {
    connection: ConnectionOptions,
    table: []const u8,
    mode: ?enum { Append, Truncate, Create } = .Append,
    sql: ?[]const u8 = null,
    create_sql: ?[]const u8 = null,
    batch_size: u64 = 10_000,
};

pub const Options = union(enum) {
    Source: SourceOptions,
    Sink: SinkOptions,
};
