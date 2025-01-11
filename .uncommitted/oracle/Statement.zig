const std = @import("std");
const debug = std.debug;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const commons = @import("../../commons.zig");
const Record = commons.Record;
const FieldValue = commons.FieldValue;
const c = @import("c.zig").c;
const t = @import("testing/testing.zig");

const Connection = @import("Connection.zig");
// const QueryMetadata = @import("../metadata/QueryMetadata.zig");

const Self = @This();
const StatementError = error{
    PrepareStatementError,
    ExecuteStatementError,
    FetchStatementError,
    StatementConfigError,
    BindStatementError,
};

allocator: Allocator,
conn: *Connection,
stmt: ?*c.dpiStmt = undefined,
column_count: u32 = 0,
sql: []const u8 = "",
found: c_int = 0,
fetch_size: u32 = c.DPI_DEFAULT_FETCH_ARRAY_SIZE,

pub fn init(allocator: Allocator, conn: *Connection) Self {
    return Self{
        .allocator = allocator,
        .stmt = null,
        .conn = conn,
    };
}

pub fn setFetchSize(self: *Self, fetch_size: u32) StatementError!void {
    // defaults to DPI_DEFAULT_FETCH_ARRAY_SIZE
    if (fetch_size > 0) {
        if (c.dpiStmt_setFetchArraySize(self.stmt, fetch_size) < 0) {
            debug.print("Failed to set fetch array size with error: {s}\n", .{self.conn.getErrorMessage()});
            return StatementError.StatementConfigError;
        }
        self.fetch_size = fetch_size;
    }
}

pub fn getFetchSize(self: *Self) StatementError!u32 {
    var fetch_size: u32 = 0;
    if (c.dpiStmt_getFetchArraySize(self.stmt, &fetch_size) < 0) {
        debug.print("Failed to get fetch array size with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.StatementConfigError;
    }
    return fetch_size;
}

pub fn prepare(self: *Self, sql: []const u8) StatementError!void {
    self.sql = sql;
    if (c.dpiConn_prepareStmt(self.conn.conn, 0, self.sql.ptr, @intCast(self.sql.len), null, 0, &self.stmt) < 0) {
        debug.print("Failed to prepare statement with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.PrepareStatementError;
    }
}

pub fn execute(self: *Self) StatementError!void {
    if (c.dpiStmt_execute(self.stmt, c.DPI_MODE_EXEC_DEFAULT, &self.column_count) < 0) {
        return StatementError.ExecuteStatementError;
    }
}
pub fn batchInsert(self: *Self, records: []Record) !void {
    if (records.len == 0) return;

    var dpi_val_array: []*c.dpiData = try self.allocator.alloc(*c.dpiData, self.column_count);
    var dpi_var_array: []*c.dpiVar = try self.allocator.alloc(*c.dpiVar, self.column_count);

    var dpi_oracle_type_number_array: []c.dpiOracleTypeNum = try self.allocator.alloc(c.dpiOracleTypeNum, self.column_count);
    var dpi_native_type_number_array: []c.dpiNativeTypeNum = try self.allocator.alloc(c.dpiNativeTypeNum, self.column_count);

    const ref = records[0];
    for (0..self.column_count) |col| {
        switch (ref[col]) {
            .String => |v| {
                dpi_oracle_type_number_array[col] = c.DPI_ORACLE_TYPE_VARCHAR;
                dpi_native_type_number_array[col] = if (v) |_| c.DPI_NATIVE_TYPE_BYTES else c.DPI_NATIVE_TYPE_NULL;
            },
            .Double => |v| {
                dpi_oracle_type_number_array[col] = c.DPI_ORACLE_TYPE_NUMBER;
                // dpi_native_type_number_array[col] = c.DPI_NATIVE_TYPE_DOUBLE;
                dpi_native_type_number_array[col] = if (v) |_| c.DPI_NATIVE_TYPE_DOUBLE else c.DPI_NATIVE_TYPE_NULL;
            },
            .Int => |v| {
                dpi_oracle_type_number_array[col] = c.DPI_ORACLE_TYPE_NUMBER;
                dpi_native_type_number_array[col] = if (v) |_| c.DPI_NATIVE_TYPE_INT64 else c.DPI_NATIVE_TYPE_NULL;
            },
            .TimeStamp => |v| {
                dpi_oracle_type_number_array[col] = c.DPI_ORACLE_TYPE_TIMESTAMP;
                dpi_native_type_number_array[col] = if (v) |_| c.DPI_NATIVE_TYPE_TIMESTAMP else c.DPI_NATIVE_TYPE_NULL;
            },
            .Boolean => |v| {
                dpi_oracle_type_number_array[col] = c.DPI_ORACLE_TYPE_NUMBER;
                dpi_native_type_number_array[col] = if (v) |_| c.DPI_NATIVE_TYPE_INT64 else c.DPI_NATIVE_TYPE_NULL;
            },
            .Number => |v| {
                dpi_oracle_type_number_array[col] = c.DPI_ORACLE_TYPE_NUMBER;
                dpi_native_type_number_array[col] = if (v) |_| c.DPI_NATIVE_TYPE_DOUBLE else c.DPI_NATIVE_TYPE_NULL;
            },
            else => unreachable,
        }
    }

    for (0..self.column_count) |col| {
        if (c.dpiConn_newVar(
            self.conn.conn,
            dpi_oracle_type_number_array[col],
            dpi_native_type_number_array[col],
            @intCast(records.len),
            0,
            0,
            0,
            null,
            &dpi_var_array[col],
            &dpi_val_array[col],
        ) < 0) {
            debug.print("Failed to create var with error: {s}\n", .{self.conn.getErrorMessage()});
            return StatementError.BindStatementError;
        }
        if (c.dpiStmt_bindByPos(self.stmt, @as(u32, @intCast(col)) + 1, dpi_var_array[col]) < 0) {
            debug.print("Failed to bind var with error: {s}\n", .{self.conn.getErrorMessage()});
            return StatementError.BindStatementError;
        }
    }

    const columnar: [][]*c.dpiData = try self.allocator.alloc([]*c.dpiData, self.column_count);
    for (columnar) |*col| {
        col.* = try self.allocator.alloc(*c.dpiData, records.len);
    }
    for (0..self.column_count) |col| {
        for (0..records.len) |rec| {
            const ptr = columnar[col][rec];
            switch (records[rec][col]) {
                .String => |v| {
                    if (v) |val| {
                        c.dpiData_setBytes(ptr, val.ptr, @intCast(val.len));
                    } else {
                        c.dpiData_setNull(ptr);
                    }
                },
                .Double => |v| {
                    if (v) |val| {
                        c.dpiData_setDouble(ptr, val);
                    } else {
                        c.dpiData_setNull(ptr);
                    }
                },
                .Int => |v| {
                    if (v) |val| {
                        c.dpiData_setInt64(ptr, val);
                    } else {
                        c.dpiData_setNull(ptr);
                    }
                },
                .Number => |v| {
                    if (v) |val| {
                        c.dpiData_setDouble(ptr, val);
                    } else {
                        c.dpiData_setNull(ptr);
                    }
                },
                .Boolean => |v| {
                    if (v) |val| {
                        c.dpiData_setBool(ptr, if (val) 1 else 0);
                    } else {
                        c.dpiData_setNull(ptr);
                    }
                },
                .TimeStamp => |v| {
                    if (v) |val| {
                        c.dpiData_setTimestamp(
                            ptr,
                            @intCast(val.year),
                            @intCast(val.month),
                            @intCast(val.day),
                            @intCast(val.hour),
                            @intCast(val.minute),
                            @intCast(val.second),
                            0,
                            0,
                            0,
                        );
                    } else {
                        c.dpiData_setNull(ptr);
                    }
                },
                else => {
                    debug.print("NotImplemented column type. col: {d}\n", .{col});
                    return StatementError.BindStatementError;
                },
            }
        }
    }

    for (0..self.column_count) |col| {
        if (c.dpiStmt_bindByPos(self.stmt, @as(u32, @intCast(col)) + 1, columnar[col]) < 0) {
            debug.print("Failed to bind column {d} with error: {s}\n", .{ col, self.conn.getErrorMessage() });
            return StatementError.BindStatementError;
        }
    }

    if (c.dpiStmt_executeMany(self.stmt, c.DPI_MODE_EXEC_DEFAULT, @intCast(records.len)) < 0) {
        debug.print("Failed to execute bulk insert with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.ExecuteStatementError;
    }
}

// pub fn batchInsert(self: *Self, records: []Record) !void {
//     const columnar: [][]*c.dpiData = try self.allocator.alloc([]*c.dpiData, self.column_count);
//     for (columnar) |*col| {
//         col.* = try self.allocator.alloc(*c.dpiData, records.len);
//     }
//     for (0..self.column_count) |col| {
//         for (0..records.len) |rec| {
//             const ptr = columnar[col][rec];
//             switch (records[rec][col]) {
//                 .String => |v| c.dpiData_setBytes(ptr, v.ptr, @intCast(v.len)),
//                 .Double => |v| c.dpiData_setDouble(ptr, v),
//                 .Int => |v| c.dpiData_setInt64(ptr, v),
//                 .Number => |v| c.dpiData_setDouble(ptr, v),
//                 .Boolean => |v| c.dpiData_setBool(ptr, if (v) 1 else 0),
//                 .TimeStamp => |v| {
//                     c.dpiData_setTimestamp(
//                         ptr,
//                         @intCast(v.year),
//                         @intCast(v.month),
//                         @intCast(v.day),
//                         @intCast(v.hour),
//                         @intCast(v.minute),
//                         @intCast(v.second),
//                         0,
//                         0,
//                         0,
//                     );
//                 },
//                 .Nil => c.dpiData_setNull(ptr),
//                 else => {
//                     debug.print("NotImplemented column type. col: {d}\n", .{col});
//                     return StatementError.BindStatementError;
//                 },
//             }
//         }
//     }

//     for (0..self.column_count) |col| {
//         if (c.dpiStmt_bindByPos(self.stmt, @as(u32, @intCast(col)) + 1, columnar[col]) < 0) {
//             debug.print("Failed to bind column {d} with error: {s}\n", .{ col, self.conn.getErrorMessage() });
//             return StatementError.BindStatementError;
//         }
//     }

//     if (c.dpiStmt_executeMany(self.stmt, c.DPI_MODE_EXEC_DEFAULT, @intCast(records.len)) < 0) {
//         debug.print("Failed to execute bulk insert with error: {s}\n", .{self.conn.getErrorMessage()});
//         return StatementError.ExecuteStatementError;
//     }
// }

pub fn fetch(self: *Self) !?Record {
    var buffer_row_index: u32 = 0;
    var native_type_num: c.dpiNativeTypeNum = 0;

    if (c.dpiStmt_fetch(self.stmt, &self.found, &buffer_row_index) < 0) {
        debug.print("Failed to fetch rows with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.FetchStatementError;
    }
    if (self.found == 0) {
        return null;
    }
    var row: Record = try self.allocator.alloc(FieldValue, self.column_count);

    for (1..self.column_count + 1) |i| {
        var data: ?*c.dpiData = undefined;
        if (c.dpiStmt_getQueryValue(self.stmt, @intCast(i), &native_type_num, &data) < 0) {
            std.debug.print("Failed to get query value with error: {s}\n", .{self.conn.getErrorMessage()});
            return error.FetchStatementError;
        }

        var value: FieldValue = undefined;

        if (data.?.isNull == 0) {
            switch (native_type_num) {
                c.DPI_NATIVE_TYPE_BYTES => {
                    value = .{
                        .String = try self.allocator.dupe(u8, data.?.value.asBytes.ptr[0..data.?.value.asBytes.length]),
                    };
                },
                c.DPI_NATIVE_TYPE_FLOAT, c.DPI_NATIVE_TYPE_DOUBLE => {
                    value = .{ .Double = data.?.value.asDouble };
                },
                c.DPI_NATIVE_TYPE_INT64 => {
                    value = .{ .Int = data.?.value.asInt64 };
                },
                c.DPI_NATIVE_TYPE_BOOLEAN => {
                    value = .{ .Boolean = data.?.value.asBoolean > 0 };
                },
                c.DPI_NATIVE_TYPE_TIMESTAMP => {
                    // todo
                    const ts = data.?.value.asTimestamp;
                    value = .{ .TimeStamp = .{
                        .day = ts.day,
                        .hour = ts.hour,
                        .minute = ts.minute,
                        .month = ts.month,
                        .second = ts.second,
                        .year = @intCast(ts.year),
                    } };
                },
                else => {
                    debug.print("Failed to get query value with error: {s}\n", .{self.conn.getErrorMessage()});
                    return error.FetchStatementError;
                },
            }
        }
        row[i - 1] = value;
    }

    return row;
}

pub fn release(self: *Self) void {
    _ = c.dpiStmt_release(self.stmt);
}

// test "batchInsert" {
//     var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arena.deinit();
//     const allocator = arena.allocator();

//     var conn = t.getTestConnection(allocator) catch unreachable;
//     conn.execute("drop table test_table") catch |err| {
//         _ = std.mem.indexOf(u8, conn.getErrorMessage(), "ORA-00942") orelse return err;
//     };
//     try conn.execute("create table test_table (id number, name varchar2(255))");
//     const sql = "insert into test_table (id, name) values (:1, :2)";
//     var stmt = try conn.prepareStatement(sql);
//     try stmt.execute();
//     var records = allocator.alloc(Record, 2) catch unreachable;
//     records[0][0] = FieldValue{ .Double = 1 };
//     records[0][1] = FieldValue{ .String = @constCast("hello") };
//     records[1][0] = FieldValue{ .Double = 2 };
//     records[1][1] = FieldValue{ .String = @constCast("world") };

//     try stmt.batchInsert(records);
//     try conn.commit();
//     stmt.release();

//     const sql2 = "select * from test_table order by id";
//     stmt = try conn.prepareStatement(sql2);
//     try stmt.execute();
//     var row = try stmt.fetch() orelse unreachable;
//     try testing.expectEqual(row.len, 2);
//     try testing.expectEqual(row[0].Double, 1);
//     try testing.expectEqualStrings(row[1].String, "hello");
//     row = try stmt.fetch() orelse unreachable;
//     try testing.expectEqual(row.len, 2);
//     try testing.expectEqual(row[0].Double, 2);
//     try testing.expectEqualStrings(row[1].String, "world");
//     stmt.release();

//     conn.execute("drop table test_table") catch |err| {
//         _ = std.mem.indexOf(u8, conn.getErrorMessage(), "ORA-00942") orelse return err;
//     };
// }

// test "fetch" {
//     var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arena.deinit();
//     const sql =
//         \\select
//         \\1 as A, 2 as B, 'hello' as C, to_date('2020-01-01', 'yyyy-mm-dd') as D
//         \\from dual
//     ;
//     var conn = t.getTestConnection(arena.allocator()) catch unreachable;

//     var stmt = try conn.prepareStatement(sql);
//     try stmt.execute();

//     var row: Record = undefined;
//     try stmt.fetch(&row);

//     try testing.expectEqual(row.len, 4);
//     try testing.expectEqual(row[0].Double, 1);
//     try testing.expectEqual(row[1].Double, 2);
//     try testing.expectEqualStrings(row[2].String, "hello");
//     try testing.expectEqual(row[3].TimeStamp.day, 1);
//     try testing.expectEqual(row[3].TimeStamp.month, 1);
//     try testing.expectEqual(row[3].TimeStamp.year, 2020);
// }
