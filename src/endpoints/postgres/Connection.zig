const Connection = @This();

const std = @import("std");
const Result = @import("Result.zig");

const c = @import("c.zig").c;
const t = @import("testing/testing.zig");

allocator: std.mem.Allocator,
username: [:0]const u8,
password: [:0]const u8,
host: [:0]const u8,
database: [:0]const u8 = "postgres",
connection_string: [:0]const u8 = undefined,

pg_conn: ?*c.PGconn = null,

const Error = error{
    ConnectionError,
    SQLExecuteError,
};

pub fn init(
    allocator: std.mem.Allocator,
    username: [:0]const u8,
    password: [:0]const u8,
    host: [:0]const u8,
    database: [:0]const u8,
) Connection {
    return .{
        .allocator = allocator,
        .username = username,
        .password = password,
        .host = host,
        .database = database,
        .connection_string = std.fmt.allocPrintZ(
            allocator,
            "host={s} user={s} password={s} dbname={s}",
            .{ host, username, password, database },
        ) catch unreachable,
    };
}

pub fn deinit(self: *Connection) void {
    self.allocator.free(self.connection_string);
    if (self.pg_conn) |conn| {
        c.PQfinish(conn);
        self.pg_conn = null;
    }
}
test "Connection.init" {
    var conn = Connection.init(
        std.testing.allocator,
        "username",
        "password",
        "host",
        "database",
    );
    defer conn.deinit();
}

pub fn connect(self: *Connection) !void {
    self.pg_conn = c.PQconnectdb(self.connection_string.ptr);
    if (self.pg_conn) |conn| {
        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            std.debug.print("Connection failed: {s}\n", .{c.PQerrorMessage(conn)});
            c.PQfinish(conn);
            return error.ConnectionError;
        }
    } else {
        return error.ConnectionError;
    }
}
test "Connection.connect" {
    const allocator = std.testing.allocator;
    var conn = t.connection(allocator) catch unreachable;
    defer conn.deinit();
    try conn.connect();
}

pub fn execute(self: Connection, sql: []const u8) !Result {
    const res = c.PQexec(self.pg_conn, sql.ptr);
    if (res) |r| {
        if (c.PQresultStatus(r) != c.PGRES_TUPLES_OK) {
            return error.SQLExecuteError;
        }
    } else {
        return error.SQLExecuteError;
    }

    return Result.init(self.allocator, res);
}
test "execute" {
    const allocator = std.testing.allocator;
    var conn = t.connection(allocator) catch unreachable;
    defer conn.deinit();
    try conn.connect();
    const result = try conn.execute("SELECT 1");
    defer result.deinit();
}
