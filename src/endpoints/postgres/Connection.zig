const std = @import("std");
pub const c = @import("c.zig").c;
const t = @import("testing/testutils.zig");

const Self = @This();

allocator: std.mem.Allocator,
username: []const u8,
password: []const u8,
host: []const u8,
database: []const u8 = "postgres",

connection_string: ?[]const u8 = null,
conn: ?*c.PGconn = null,

pub fn init(
    allocator: std.mem.Allocator,
    username: []const u8,
    password: []const u8,
    host: []const u8,
    database: []const u8,
) Self {
    return .{
        .allocator = allocator,
        .username = username,
        .password = password,
        .host = host,
        .database = database,
        .connection_string = std.fmt.allocPrint(
            allocator,
            "host={s} user={s} password={s} dbname={s}",
            .{ host, username, password, database },
        ) catch unreachable,
    };
}

pub fn deinit(self: *Self) void {
    if (self.connection_string) |cs| self.allocator.free(cs);
    self.disconnect();
}

test "Connection.init" {
    var conn = Self.init(
        std.testing.allocator,
        "username",
        "password",
        "host",
        "database",
    );
    defer conn.deinit();
}

pub fn connect(self: *Self) !void {
    self.conn = c.PQconnectdb(self.connection_string.?.ptr);
}
pub fn disconnect(self: *Self) void {
    if (self.conn) |conn| {
        c.PQfinish(conn);
        self.conn = null;
    }
}
test "Connection.connect" {
    const allocator = std.testing.allocator;
    var conn = t.connection(allocator) catch unreachable;
    defer conn.deinit();
    try conn.connect();
}
