const Statement = @This();

const std = @import("std");
const Connection = @import("Connection.zig");
const Column = @import("metadata/Column.zig");
const CursorMetadata = @import("metadata/CursorMetadata.zig");
const QueryMetadata = @import("../shared/db/metadata/Query.zig");

const p = @import("../../wire/proto/proto.zig");
const c = @import("c.zig").c;
const e = @import("error.zig");
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
        std.debug.print("Error executing BEGIN: {s}\n", .{e.resultError(begin_res)});
        return error.TransactionError;
    }
    c.PQclear(begin_res);

    const cursor_script = try std.fmt.allocPrintZ(self.allocator, "DECLARE {s} CURSOR FOR {s}", .{ name, self.sql });
    defer self.allocator.free(cursor_script);

    const cursor_res = c.PQexec(self.conn.pg_conn, cursor_script);
    if (c.PQresultStatus(cursor_res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing cursor: {s}\n", .{e.resultError(cursor_res)});
        return error.TransactionError;
    }
    c.PQclear(cursor_res);
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
        std.debug.print("Error executing cursor: {s}\n", .{e.resultError(res)});
        return error.TransactionError;
    }
    c.PQclear(res);
}
test "Statement.testFetch" {
    const allocator = std.testing.allocator;
    const sql =
        \\select 1 as A, 2 as B, 'hello' as C, null as D, cast(null as integer) as E
        \\union
        \\select 3 as A, 4 as B, 'world' as C, null as D, cast(null as integer) as E
    ;
    var conn = t.connection(allocator);
    try conn.connect();
    defer conn.deinit();

    var stmt = try conn.createStatement(sql);
    defer stmt.deinit();

    const cursor_name = "my_cursor";
    try stmt.createCursor(cursor_name);
    var md = try stmt.createCursorMetadata(cursor_name);
    defer md.deinit();

    {
        // fetch test
        const fetch_sql = try std.fmt.allocPrintZ(
            allocator,
            "FETCH {d} FROM {s}",
            .{ stmt.fetch_size, cursor_name },
        );
        defer stmt.allocator.free(fetch_sql);

        while (true) {
            const res = c.PQexec(stmt.conn.pg_conn, fetch_sql);
            if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
                break;
            }
            const rows = c.PQntuples(res);
            if (rows == 0) break;

            for (0..@intCast(rows)) |ri| {
                for (md.columns) |column| {
                    // const cv1 = c.PQgetvalue(res, @intCast(ri), @intCast(column.index));
                    const is_null = c.PQgetisnull(res, @intCast(ri), @intCast(column.index));

                    const cv = if (is_null != 1) std.mem.span(c.PQgetvalue(res, @intCast(ri), @intCast(column.index))) else null;

                    const val: p.Value = column.type.stringToValue(stmt.allocator, cv);
                    defer val.deinit(stmt.allocator);

                    if (ri == 0) {
                        if (column.index == 0) {
                            try std.testing.expectEqual(val, p.Value{ .Int = 1 });
                        }
                        if (column.index == 2) {
                            try std.testing.expectEqualStrings(val.String.?, "hello");
                        }
                        if (column.index == 3) {
                            try std.testing.expectEqual(val, p.Value{ .String = null });
                            // std.debug.print("---> CV: {any}\n", .{p.Value{ .String = null }});
                            // std.debug.print("---> VAl: {any}\n", .{val});
                            // std.debug.print("---> is_null: {d}\n", .{is_null});
                        }
                        if (column.index == 4) {
                            try std.testing.expectEqual(val, p.Value{ .Int = null });
                        }
                    }
                }
            }

            c.PQclear(res);
        }
    }
    try stmt.closeCursor(cursor_name);
    try stmt.conn.commit();
}

pub fn createCursorMetadata(self: *Statement, name: []const u8) !CursorMetadata {
    const sql = try std.fmt.allocPrintZ(self.allocator, "FETCH {d} FROM {s}", .{ 0, name });
    defer self.allocator.free(sql);

    const res = c.PQexec(self.conn.pg_conn, sql);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        std.debug.print("Error executing FETCH: {s}\n", .{e.resultError(res)});
        return error.TransactionError;
    }
    return try CursorMetadata.init(self.allocator, name, res.?);
}
test "Statement.createCursorMetadata" {
    const allocator = std.testing.allocator;
    const sql =
        \\select 1 as A, 2 as B, 'hello' as C
        \\union
        \\select 3 as A, 4 as B, 'world' as C
    ;
    var conn = t.connection(allocator);
    try conn.connect();
    defer conn.deinit();

    var stmt = try conn.createStatement(sql);
    defer stmt.deinit();

    const cursor_name = "my_cursor";
    try stmt.createCursor(cursor_name);
    const md = try stmt.createCursorMetadata(cursor_name);
    defer md.deinit();

    try std.testing.expectEqual(md.columns.len, 3);
    try std.testing.expectEqualStrings(md.columns[0].name, "a");
    try std.testing.expectEqualStrings(md.columns[1].name, "b");
    try std.testing.expectEqualStrings(md.columns[2].name, "c");

    try stmt.closeCursor(cursor_name);
    try stmt.conn.commit();
}
