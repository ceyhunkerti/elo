const Statement = @This();

const std = @import("std");
const Connection = @import("Connection.zig");

const c = @import("c.zig").c;
const p = @import("../../wire/proto/proto.zig");
const e = @import("error.zig");
const t = @import("testing/testing.zig");

allocator: std.mem.Allocator = undefined,
dpi_stmt: ?*c.dpiStmt = null,

conn: *Connection = undefined,

pub fn init(allocator: std.mem.Allocator, conn: *Connection) Statement {
    return .{
        .allocator = allocator,
        .conn = conn,
    };
}

pub fn deinit(self: *Statement) void {
    self.release();
}

pub fn release(self: *Statement) void {
    e.check(c.dpiStmt_release(self.dpi_stmt), error.Fail) catch {
        std.debug.print("Failed to release statement with error: {s}\n", .{self.conn.context.errorMessage()});
        unreachable;
    };
    self.dpi_stmt = null;
}

pub fn prepare(self: *Statement, sql: []const u8) !void {
    try e.check(
        c.dpiConn_prepareStmt(self.conn.dpi_conn, 0, sql.ptr, @intCast(sql.len), null, 0, &self.dpi_stmt),
        error.Fail,
    );
}

pub fn setFetchSize(self: *Statement, fetch_size: u32) !void {
    if (fetch_size > 0) {
        try e.check(
            c.dpiStmt_setFetchArraySize(self.dpi_stmt, fetch_size),
            error.Fail,
        );
    }
}

pub fn execute(self: *Statement) !u32 {
    var column_count: u32 = 0;
    try e.check(
        c.dpiStmt_execute(self.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, &column_count),
        error.Fail,
    );
    return column_count;
}

pub fn fetch(self: *Statement, column_count: u32) !?p.Record {
    var buffer_row_index: u32 = 0;
    var native_type_num: c.dpiNativeTypeNum = 0;
    var found: c_int = 0;

    if (c.dpiStmt_fetch(self.dpi_stmt, &found, &buffer_row_index) < 0) {
        return error.Fail;
    }
    if (found == 0) {
        return null;
    }
    var record = try p.Record.init(self.allocator, column_count);

    for (1..column_count + 1) |i| {
        var data: ?*c.dpiData = undefined;
        if (c.dpiStmt_getQueryValue(self.dpi_stmt, @intCast(i), &native_type_num, &data) < 0) {
            return error.Fail;
        }
        var value: p.Value = undefined;
        switch (native_type_num) {
            c.DPI_NATIVE_TYPE_BYTES => {
                const ptr = c.dpiData_getBytes(@ptrCast(data));
                value = .{
                    .Bytes = if (data.?.isNull != 0) null else try self.allocator.dupe(
                        u8,
                        ptr.*.ptr[0..ptr.*.length],
                    ),
                };
                // if (value.Bytes) |bytes|
                //     std.debug.print("V: {any} {s}\n", .{ value, bytes });
            },
            c.DPI_NATIVE_TYPE_FLOAT, c.DPI_NATIVE_TYPE_DOUBLE => {
                value = .{ .Double = if (data.?.isNull != 0) null else data.?.value.asDouble };
            },
            c.DPI_NATIVE_TYPE_INT64 => {
                value = .{ .Int = if (data.?.isNull != 0) null else data.?.value.asInt64 };
            },
            c.DPI_NATIVE_TYPE_BOOLEAN => {
                value = .{ .Boolean = if (data.?.isNull != 0) null else data.?.value.asBoolean > 0 };
            },
            c.DPI_NATIVE_TYPE_TIMESTAMP => {
                if (data.?.isNull != 0) {
                    value = .{ .TimeStamp = null };
                } else {
                    const ts = data.?.value.asTimestamp;
                    value = .{ .TimeStamp = .{
                        .day = ts.day,
                        .hour = ts.hour,
                        .minute = ts.minute,
                        .month = ts.month,
                        .second = ts.second,
                        .year = @intCast(ts.year),
                    } };
                }
            },
            else => {
                std.debug.print("Unsupported native type num: {d}\n", .{native_type_num});
                return error.Fail;
            },
        }
        try record.append(value);
    }
    return record;
}

test "Statement.fetch" {
    const allocator = std.testing.allocator;
    const sql =
        \\select
        \\1 as A, 2 as B, 'hello' as C, to_date('2020-01-01', 'yyyy-mm-dd') as D
        \\from dual
    ;
    const tp = try t.ConnectionParams.initFromEnv(allocator);
    var conn = tp.toConnection();
    try conn.connect();
    defer conn.deinit();

    var stmt = try conn.prepareStatement(sql);
    defer stmt.deinit();
    try stmt.setFetchSize(1);

    const record = try stmt.fetch(try stmt.execute());
    try std.testing.expect(record != null);
    if (record) |r| {
        defer r.deinit(allocator);
        try std.testing.expectEqual(r.len(), 4);
        try std.testing.expectEqual(r.items()[0].Double, 1);
        try std.testing.expectEqual(r.items()[1].Double, 2);
        try std.testing.expectEqualStrings(r.items()[2].Bytes.?, "hello");
        try std.testing.expectEqual(r.items()[3].TimeStamp.?.day, 1);
        try std.testing.expectEqual(r.items()[3].TimeStamp.?.month, 1);
        try std.testing.expectEqual(r.items()[3].TimeStamp.?.year, 2020);
    }
}

pub fn executeMany(self: *Statement, num_iters: u32) !void {
    try e.check(
        c.dpiStmt_executeMany(self.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, num_iters),
        error.Fail,
    );
}
