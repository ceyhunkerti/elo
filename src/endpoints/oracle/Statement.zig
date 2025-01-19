const std = @import("std");

const Connection = @import("Connection.zig");
const c = @import("c.zig").c;
const p = @import("../../wire/proto.zig");
const Record = p.Record;
const Value = p.Value;

const t = @import("testing/testing.zig");
const checkError = @import("./utils.zig").checkError;

const Self = @This();

allocator: std.mem.Allocator = undefined,
connection: *Connection = undefined,
dpi_stmt: ?*c.dpiStmt = null,

pub const Error = error{
    StatementConfigError,
    ExecuteStatementError,
    FetchStatementError,
    StatementReleaseError,
};

pub fn init(allocator: std.mem.Allocator, connection: *Connection) Self {
    return .{
        .allocator = allocator,
        .connection = connection,
    };
}

pub fn release(self: *Self) !void {
    try checkError(
        c.dpiStmt_release(self.dpi_stmt),
        error.StatementReleaseError,
    );
    self.dpi_stmt = null;
}

pub fn prepare(self: *Self, sql: []const u8) !void {
    try checkError(
        c.dpiConn_prepareStmt(self.connection.dpi_conn, 0, sql.ptr, @intCast(sql.len), null, 0, &self.dpi_stmt),
        error.PrepareStatementError,
    );
}

pub fn setFetchSize(self: *Self, fetch_size: u32) !void {
    if (fetch_size > 0) {
        try checkError(
            c.dpiStmt_setFetchArraySize(self.dpi_stmt, fetch_size),
            error.StatementConfigError,
        );
    }
}

pub fn execute(self: *Self) !u32 {
    var column_count: u32 = 0;
    try checkError(
        c.dpiStmt_execute(self.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, &column_count),
        error.ExecuteStatementError,
    );
    return column_count;
}

pub fn fetch(self: *Self, column_count: u32) !?Record {
    var buffer_row_index: u32 = 0;
    var native_type_num: c.dpiNativeTypeNum = 0;
    var found: c_int = 0;

    if (c.dpiStmt_fetch(self.dpi_stmt, &found, &buffer_row_index) < 0) {
        return error.FetchStatementError;
    }
    if (found == 0) {
        return null;
    }
    var record = try Record.init(self.allocator, column_count);

    for (1..column_count + 1) |i| {
        var data: ?*c.dpiData = undefined;
        if (c.dpiStmt_getQueryValue(self.dpi_stmt, @intCast(i), &native_type_num, &data) < 0) {
            return error.FetchStatementError;
        }
        var value: Value = undefined;
        switch (native_type_num) {
            c.DPI_NATIVE_TYPE_BYTES => {
                value = .{
                    .String = if (data.?.isNull != 0) null else try self.allocator.dupe(
                        u8,
                        data.?.value.asBytes.ptr[0..data.?.value.asBytes.length],
                    ),
                };
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
                return error.FetchStatementError;
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
    var conn = t.getTestConnection(allocator) catch unreachable;
    try conn.connect();

    var stmt = try conn.prepareStatement(sql);
    const record = try stmt.fetch(try stmt.execute());

    try std.testing.expect(record != null);
    if (record) |r| {
        defer r.deinit(allocator);
        try std.testing.expectEqual(r.len(), 4);
        try std.testing.expectEqual(r.items()[0].Double, 1);
        try std.testing.expectEqual(r.items()[1].Double, 2);
        try std.testing.expectEqualStrings(r.items()[2].String.?, "hello");
        try std.testing.expectEqual(r.items()[3].TimeStamp.?.day, 1);
        try std.testing.expectEqual(r.items()[3].TimeStamp.?.month, 1);
        try std.testing.expectEqual(r.items()[3].TimeStamp.?.year, 2020);
    }
    try stmt.release();
    try conn.deinit();
}

pub fn executeMany(self: *Self, num_iters: u32) !void {
    try checkError(
        c.dpiStmt_executeMany(self.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, num_iters),
        error.ExecuteStatementError,
    );
}
