const Statement = @This();

const Connection = @import("Connection.zig");
const std = @import("std");

const c = @import("c.zig").c;
const t = @import("testing/testing.zig");

const FETCH_SIZE = 10_000;

allocator: std.mem.Allocator,
conn: *Connection,
sql: []const u8,
fetch_size: u32 = FETCH_SIZE,

pub const Error = error{TransactionError};

pub fn init(allocator: std.mem.Allocator, conn: *Connection, sql: []const u8) Statement {
    return .{
        .allocator = allocator,
        .conn = conn,
        .sql = allocator.dupe(u8, sql) catch unreachable,
    };
}
pub fn deinit(self: Statement) void {
    self.allocator.free(self.sql);
}

pub fn setFetchSize(self: *Statement, fetch_size: u32) void {
    self.fetch_size = fetch_size;
}

pub fn createCursor(self: *Statement, name: []const u8) !void {
    const begin_res = c.PQexec(self.conn.pg_conn, "BEGIN");
    if (c.PQresultStatus(begin_res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing BEGIN: {s}\n", .{self.conn.errorMessage()});
        return error.TransactionError;
    }
    // c.PQclear(begin_res);

    const cursor_script = try std.fmt.allocPrintZ(self.allocator, "DECLARE {s} CURSOR FOR {s}", .{ name, self.sql });
    defer self.allocator.free(cursor_script);

    const cursor_res = c.PQexec(self.conn.pg_conn, cursor_script);
    if (c.PQresultStatus(cursor_res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing cursor: {s}\n", .{self.conn.errorMessage()});
        return error.TransactionError;
    }
    // c.PQclear(cursor_res);
}

pub fn closeCursor(self: Statement, name: []const u8) !void {
    const sql = try std.fmt.allocPrintZ(
        self.allocator,
        "CLOSE {s}",
        .{name},
    );
    defer self.allocator.free(sql);
    const res = c.PQexec(self.conn.pg_conn, sql);
    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing cursor: {s}\n", .{self.conn.errorMessage()});
        return error.TransactionError;
    }
    c.PQclear(res);
}

pub fn fetch(self: Statement, cursor_name: []const u8) !void {
    const sql = try std.fmt.allocPrintZ(
        self.allocator,
        "FETCH {d} FROM {s}",
        .{ self.fetch_size, cursor_name },
    );
    defer self.allocator.free(sql);

    while (true) {
        const res = c.PQexec(self.conn.pg_conn, sql);
        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
            break;
        }
        const rows = c.PQntuples(res);
        if (rows == 0) break;
        c.PQclear(res);
    }
}
test "Statement.fetch" {
    const allocator = std.testing.allocator;
    const sql =
        \\select 1 as A, 2 as B, 'hello' as C
        \\union
        \\select 3 as A, 4 as B, 'world' as C
    ;
    var conn = try t.connection(allocator);
    try conn.connect();
    defer conn.deinit();

    var stmt = try conn.createStatement(sql);
    defer stmt.deinit();

    const cursor_name = "my_cursor";
    try stmt.createCursor(cursor_name);
    try stmt.fetch(cursor_name);
    try stmt.closeCursor(cursor_name);
    try stmt.conn.commit();
}
