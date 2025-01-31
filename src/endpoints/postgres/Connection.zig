const Connection = @This();

const std = @import("std");
const Cursor = @import("Cursor.zig");

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
    CommitError,
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
    if (self.isConnected()) {
        c.PQfinish(self.pg_conn);
    }
    self.pg_conn = null;
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
    var conn = t.connection(allocator);
    defer conn.deinit();
    try conn.connect();
}

pub fn isConnected(self: Connection) bool {
    if (self.pg_conn) |conn| {
        return c.PQstatus(conn) == c.CONNECTION_OK;
    }
    return false;
}

pub fn errorMessage(self: Connection) []const u8 {
    if (self.pg_conn) |_| {
        return std.mem.span(c.PQerrorMessage(self.pg_conn));
    }
    return "Not connected";
}
pub fn commit(self: Connection) !void {
    const res = c.PQexec(self.pg_conn, "COMMIT");
    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error committing transaction: {s}\n", .{self.errorMessage()});
        return error.CommitError;
    }
    c.PQclear(res);
}

pub fn createCursor(self: *Connection, name: []const u8, sql: []const u8) !Cursor {
    return try Cursor.init(self.allocator, self, name, sql);
}

pub fn execute(self: Connection, sql: []const u8) !void {
    const res = c.PQexec(self.pg_conn, sql.ptr);
    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing SQL: {s}\n", .{self.errorMessage()});
        return error.SQLExecuteError;
    }
    c.PQclear(res);
}
