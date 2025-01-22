const std = @import("std");
const Statement = @import("Statement2.zig");
const Connection = @import("Connection2.zig");
const Column = @import("metadata/Column.zig");
const ResultSetMetadata = @import("ResultSetMetadata.zig");

const p = @import("../../wire/proto.zig");
const oci = @import("c.zig").oci;
const t = @import("testing/testing.zig");

const Self = @This();

allocator: std.mem.Allocator,
oci_result_set: ?*oci.OCI_Resultset = null,

statement: *Statement,
_metadata: ?ResultSetMetadata = null,

pub fn init(allocator: std.mem.Allocator, stmt: *Statement) !Self {
    const oci_result_set: ?*oci.OCI_Resultset = oci.OCI_GetResultset(stmt.oci_stmt);
    if (oci_result_set == null) {
        return error.Fail;
    }
    return Self{
        .allocator = allocator,
        .statement = stmt,
        .oci_result_set = oci_result_set,
    };
}
pub fn deinit(self: *Self) void {
    if (self.oci_result_set != null) {
        self.oci_result_set = null;
    }
    if (self._metadata) |*md| {
        md.deinit();
        self._metadata = null;
    }
}

pub fn fetchNext(self: Self) !void {
    if (oci.OCI_FetchNext(self.oci_result_set) != oci.TRUE) {
        return error.Fail;
    }
}
pub fn getColumnCount(self: Self) u32 {
    return oci.OCI_GetColumnCount(self.oci_result_set);
}

test "ResultSet.getColumnCount" {
    const params = try t.ConnectionParams.init();
    var conn = try Connection.init(
        std.testing.allocator,
        params.connection_string,
        params.username,
        params.password,
        params.privilege,
    );
    defer conn.deinit() catch unreachable;
    try conn.connect();

    var stmt = try conn.prepareStatement("select 1 as a, 'hello' as b from dual");
    defer stmt.deinit() catch unreachable;
    try stmt.execute();
    const rs = try stmt.getResultSet();
    try std.testing.expectEqual(rs.getColumnCount(), 2);
}

pub fn getColumn(self: Self, index: u32) !Column {
    const oci_column: ?*oci.OCI_Column = oci.OCI_GetColumn(self.oci_result_set, index);
    if (oci_column) |oc| {
        return Column.init(self.allocator, index, oc);
    }
    return error.Fail;
}
test "ResultSet.getColumn" {
    const params = try t.ConnectionParams.init();
    var conn = try Connection.init(
        std.testing.allocator,
        params.connection_string,
        params.username,
        params.password,
        params.privilege,
    );
    defer conn.deinit() catch unreachable;
    try conn.connect();

    var stmt = try conn.prepareStatement("select 1 as a, 'hello' as b from dual");
    defer stmt.deinit() catch unreachable;
    try stmt.execute();
    const rs = try stmt.getResultSet();
    const column = try rs.getColumn(1);

    try std.testing.expectEqualStrings("A", column.getName());
    try std.testing.expect(column.isNullable());
    try std.testing.expectEqualStrings("NUMBER", column.getSqlType());
    try std.testing.expectEqual(oci.OCI_CDT_NUMERIC, @as(c_int, @intCast(column.getType())));
}

pub fn getMetadata(self: *Self) !ResultSetMetadata {
    if (self._metadata) |meta| {
        return meta;
    }
    self._metadata = try ResultSetMetadata.init(self.allocator, self);
    return self._metadata.?;
}
test "ResultSet.getMetadata" {
    const params = try t.ConnectionParams.init();
    var conn = try Connection.init(
        std.testing.allocator,
        params.connection_string,
        params.username,
        params.password,
        params.privilege,
    );
    defer conn.deinit() catch unreachable;
    try conn.connect();

    var stmt = try conn.prepareStatement("select 1 as a, 'hello' as b from dual");
    defer stmt.deinit() catch unreachable;
    try stmt.execute();
    var rs = try stmt.getResultSet();
    defer rs.deinit();
    const md = try rs.getMetadata();

    try std.testing.expectEqualStrings("A", md.columns[0].getName());
    try std.testing.expect(md.columns[0].isNullable());
    try std.testing.expectEqualStrings("NUMBER", md.columns[0].getSqlType());
    try std.testing.expectEqual(oci.OCI_CDT_NUMERIC, @as(c_int, @intCast(md.columns[0].getType())));
}

// index starts at 1
// will be used for faster access to values otherwise
// for each value we need some metadata access overhead.
fn getValue2(self: *Self, index: u32, column_type: c_uint, column_sub_type: c_uint) !p.Value {
    switch (column_type) {
        oci.OCI_CDT_NUMERIC => switch (column_sub_type) {
            oci.OCI_NUM_DOUBLE, oci.OCI_NUM_FLOAT, oci.OCI_NUM_NUMBER => {
                return .{ .Double = oci.OCI_GetDouble(self.oci_result_set, index) };
            },
            else => return .{ .Int = oci.OCI_GetInt(self.oci_result_set, index) },
        },
        oci.OCI_CDT_TEXT => {
            const text = std.mem.sliceTo(oci.OCI_GetString(self.oci_result_set, index), 0);
            return .{ .String = try self.allocator.dupe(u8, text) };
        },
        oci.OCI_CDT_DATETIME => {
            const ot = oci.OCI_GetDate(self.oci_result_set, index);
            if (ot == null) return error.Fail;

            var year: c_int = undefined;
            var month: c_int = undefined;
            var day: c_int = undefined;
            if (oci.OCI_DateGetDate(ot, &year, &month, &day) != oci.TRUE) {
                return error.Fail;
            }
            return .{ .TimeStamp = .{
                .year = @intCast(year),
                .month = @intCast(month),
                .day = @intCast(day),
                .hour = 0,
                .minute = 0,
                .second = 0,
                .nanosecond = 0,
            } };
        },
        oci.OCI_CDT_TIMESTAMP => {
            const ots = oci.OCI_GetTimestamp(self.oci_result_set, index);
            if (ots == null) return .{ .TimeStamp = null };

            var year: c_int = undefined;
            var month: c_int = undefined;
            var day: c_int = undefined;
            var hour: c_int = undefined;
            var min: c_int = undefined;
            var sec: c_int = undefined;
            var fsec: c_int = undefined;

            if (oci.OCI_TimestampGetDateTime(ots, &year, &month, &day, &hour, &min, &sec, &fsec) != oci.TRUE) {
                return error.Fail;
            }

            switch (column_sub_type) {
                oci.OCI_TIMESTAMP => {
                    return .{ .TimeStamp = .{
                        .year = @intCast(year),
                        .month = @intCast(month),
                        .day = @intCast(day),
                        .hour = @intCast(hour),
                        .minute = @intCast(min),
                        .second = @intCast(sec),
                        .nanosecond = @intCast(fsec),
                    } };
                },
                oci.OCI_TIMESTAMP_TZ, oci.OCI_TIMESTAMP_LTZ => {
                    var tz_hour: c_int = undefined;
                    var tz_min: c_int = undefined;
                    if (oci.OCI_TimestampGetTimeZoneOffset(ots, &tz_hour, &tz_min) != oci.TRUE) {
                        return error.Fail;
                    }
                    return .{ .TimeStamp = .{
                        .year = @intCast(year),
                        .month = @intCast(month),
                        .day = @intCast(day),
                        .hour = @intCast(hour),
                        .minute = @intCast(min),
                        .second = @intCast(sec),
                        .nanosecond = @intCast(fsec),
                        .tz_offset = .{
                            .hour = @intCast(tz_hour),
                            .minute = @intCast(tz_min),
                        },
                    } };
                },
                else => unreachable,
            }
        },
        else => unreachable,
    }
    unreachable;
}

// index starts at 1
pub fn getValue(self: *Self, index: u32) !p.Value {
    const md = try self.getMetadata();
    const column_type = md.getColumnType(index);
    const column_sub_type = md.getColumnSubType(index);
    return self.getValue2(index, column_type, column_sub_type);
}
test "ResultSet.getValue" {
    const allocator = std.testing.allocator;

    const params = try t.ConnectionParams.init();
    var conn = try Connection.init(
        allocator,
        params.connection_string,
        params.username,
        params.password,
        params.privilege,
    );
    defer conn.deinit() catch unreachable;
    try conn.connect();

    var stmt = try conn.prepareStatement(
        \\select 1 as a, 'hello' as b, to_date('2020-01-21', 'yyyy-mm-dd') as c,
        \\ to_timestamp_tz('1999-12-01 11:00:00 -8:00','YYYY-MM-DD HH:MI:SS TZH:TZM') d
        \\from dual
    );
    defer stmt.deinit() catch unreachable;
    try stmt.execute();
    var rs = try stmt.getResultSet();
    defer rs.deinit();

    try rs.fetchNext();
    const val_a = try rs.getValue(1);
    try std.testing.expectEqual(@as(f64, 1.0), val_a.Double);

    const val_b = try rs.getValue(2);
    defer val_b.deinit(allocator);
    try std.testing.expectEqualStrings("hello", val_b.String.?);

    const val_c = try rs.getValue(3);
    defer val_c.deinit(allocator);
    try std.testing.expectEqual(val_c.TimeStamp.?.year, 2020);
    try std.testing.expectEqual(val_c.TimeStamp.?.month, 1);
    try std.testing.expectEqual(val_c.TimeStamp.?.day, 21);

    const val_d = try rs.getValue(4);
    defer val_d.deinit(allocator);
    try std.testing.expectEqual(val_d.TimeStamp.?.year, 1999);
    try std.testing.expectEqual(val_d.TimeStamp.?.month, 12);
    try std.testing.expectEqual(val_d.TimeStamp.?.day, 1);
    try std.testing.expectEqual(val_d.TimeStamp.?.hour, 11);
    try std.testing.expectEqual(val_d.TimeStamp.?.minute, 0);
    try std.testing.expectEqual(val_d.TimeStamp.?.second, 0);
    try std.testing.expectEqual(val_d.TimeStamp.?.tz_offset.hour, -8);
    try std.testing.expectEqual(val_d.TimeStamp.?.tz_offset.minute, 0);
}
