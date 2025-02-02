const Cursor = @This();

const std = @import("std");
const Connection = @import("Connection.zig");

const md = @import("metadata/metadata.zig");
const Column = md.Column;

const p = @import("../../wire/proto/proto.zig");
const c = @import("c.zig").c;
const e = @import("error.zig");

const FETCH_SIZE = 10_000;

pub const Metadata = struct {
    columns: []Column,

    fn getColumn(allocator: std.mem.Allocator, res: *const c.PGresult, index: u32) !Column {
        const type_oid = c.PQftype(res, @intCast(index));
        const column_name = std.mem.span(c.PQfname(res, @intCast(index)));

        return Column{
            .allocator = allocator,
            .index = index,
            .name = try allocator.dupe(u8, column_name),
            .type_info = .{
                .ext = md.pgtype.PostgresType.fromOid(type_oid) orelse {
                    std.debug.print("Error: could not find type for OID {d}\n", .{type_oid});
                    return error.Fail;
                },
            },
        };
    }

    pub fn init(allocator: std.mem.Allocator, res: *const c.PGresult) !Metadata {
        const column_count: u32 = @intCast(c.PQnfields(res));
        const columns = try allocator.alloc(Column, column_count);
        for (columns, 0..) |*column, i| {
            column.* = try getColumn(allocator, res, @intCast(i));
        }
        return .{
            .columns = columns,
        };
    }
    pub fn deinit(self: Metadata, allocator: std.mem.Allocator) void {
        for (self.columns) |*column| {
            column.deinit();
        }
        allocator.free(self.columns);
    }
};

allocator: std.mem.Allocator,
conn: *Connection,
name: []const u8,
fetch_size: u32 = FETCH_SIZE,
query: []const u8,
metadata: Metadata = undefined,
fetch_query: [:0]const u8 = undefined,

pg_result: ?*c.PGresult = null,
row_count: u32 = 0,
row_index: u32 = 0,
total_row_count: usize = 0,

pub fn init(allocator: std.mem.Allocator, conn: *Connection, name: []const u8, sql: []const u8) !Cursor {
    var cursor = Cursor{
        .allocator = allocator,
        .conn = conn,
        .name = allocator.dupe(u8, name) catch unreachable,
        .query = allocator.dupe(u8, sql) catch unreachable,
        .fetch_query = try std.fmt.allocPrintZ(allocator, "FETCH {d} FROM {s}", .{ FETCH_SIZE, name }),
    };
    try cursor.declare();
    cursor.metadata = try cursor.findMetadata();
    return cursor;
}
pub fn deinit(self: Cursor) void {
    self.allocator.free(self.name);
    self.allocator.free(self.query);
    self.allocator.free(self.fetch_query);
    self.metadata.deinit(self.allocator);
    if (self.pg_result) |res| c.PQclear(res);
}

pub fn setFetchSize(self: *Cursor, fetch_size: u32) void {
    if (fetch_size == self.fetch_size) return;
    self.fetch_size = fetch_size;
    self.allocator.free(self.fetch_query);
    self.fetch_query = try std.fmt.allocPrintZ(self.allocator, "FETCH {d} FROM {s}", .{ fetch_size, self.name });
}

fn declare(self: Cursor) !void {
    const sql = try std.fmt.allocPrintZ(self.allocator, "DECLARE {s} CURSOR FOR {s}", .{ self.name, self.query });
    defer self.allocator.free(sql);

    const res = c.PQexec(self.conn.pg_conn, sql);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing cursor: {s}\n", .{e.resultError(res)});
        return error.Fail;
    }
}

pub fn close(self: Cursor) !void {
    const sql = try std.fmt.allocPrintZ(self.allocator, "CLOSE {s}", .{self.name});
    defer self.allocator.free(sql);

    const res = c.PQexec(self.conn.pg_conn, sql);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        std.debug.print("Error executing cursor: {s}\n", .{e.resultError(res)});
        return error.Fail;
    }
}

fn findMetadata(self: Cursor) !Metadata {
    const sql = try std.fmt.allocPrintZ(self.allocator, "FETCH {d} FROM {s}", .{ 0, self.name });
    defer self.allocator.free(sql);

    const res = c.PQexec(self.conn.pg_conn, sql);
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        std.debug.print("Error executing FETCH: {s}\n", .{e.resultError(res)});
        return error.Fail;
    }
    return try Metadata.init(self.allocator, res.?);
}

pub fn execute(self: *Cursor) !u32 {
    self.row_index = 0;
    if (self.pg_result) |res| c.PQclear(res);
    self.pg_result = c.PQexec(self.conn.pg_conn, @ptrCast(self.fetch_query.ptr));
    if (c.PQresultStatus(self.pg_result) != c.PGRES_TUPLES_OK) {
        return error.Fail;
    }
    self.row_count = @intCast(c.PQntuples(self.pg_result));
    if (self.row_count == 0) {
        if (self.pg_result) |res| c.PQclear(res);
        self.pg_result = null;
        return 0;
    }
    self.total_row_count += self.row_count;
    return self.row_count;
}

pub inline fn fetchNext(self: *Cursor) !?p.Record {
    if (self.row_index >= self.row_count) return null;
    var record = try p.Record.init(self.allocator, self.metadata.columns.len);
    for (self.metadata.columns) |column| {
        const is_null = c.PQgetisnull(self.pg_result, @intCast(self.row_index), @intCast(column.index));
        const str = if (is_null != 1) std.mem.span(c.PQgetvalue(self.pg_result, @intCast(self.row_index), @intCast(column.index))) else null;
        const column_type = column.type_info.?.ext.?;
        const val: p.Value = column_type.stringToValue(self.allocator, str);
        try record.append(val);
    }
    self.row_index += 1;
    return record;
}
