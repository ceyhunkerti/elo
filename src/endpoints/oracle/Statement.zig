const std = @import("std");
const debug = std.debug;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const commons = @import("../../commons.zig");
const Record = commons.Record;
const FieldValue = commons.FieldValue;
const cc = @import("c.zig").c;
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
stmt: ?*cc.dpiStmt = undefined,
column_count: u32 = 0,
sql: []const u8 = "",
found: c_int = 0,
fetch_size: u32 = cc.DPI_DEFAULT_FETCH_ARRAY_SIZE,

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
        if (cc.dpiStmt_setFetchArraySize(self.stmt, fetch_size) < 0) {
            debug.print("Failed to set fetch array size with error: {s}\n", .{self.conn.getErrorMessage()});
            return StatementError.StatementConfigError;
        }
        self.fetch_size = fetch_size;
    }
}

pub fn getFetchSize(self: *Self) StatementError!u32 {
    var fetch_size: u32 = 0;
    if (cc.dpiStmt_getFetchArraySize(self.stmt, &fetch_size) < 0) {
        debug.print("Failed to get fetch array size with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.StatementConfigError;
    }
    return fetch_size;
}

pub fn prepare(self: *Self, sql: []const u8) StatementError!void {
    self.sql = sql;
    if (cc.dpiConn_prepareStmt(self.conn.conn, 0, self.sql.ptr, @intCast(self.sql.len), null, 0, &self.stmt) < 0) {
        debug.print("Failed to prepare statement with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.PrepareStatementError;
    }
}

pub fn execute(self: *Self) StatementError!void {
    if (cc.dpiStmt_execute(self.stmt, cc.DPI_MODE_EXEC_DEFAULT, &self.column_count) < 0) {
        return StatementError.ExecuteStatementError;
    }
}

// pub fn batch(self: *Self, records: []Record, dpi_var_array: []*cc.dpiVar) !void {
//     if (records.len == 0) return;

//     // // var dpi_val_array: []*cc.dpiData = try self.allocator.alloc(*cc.dpiData, self.column_count);
//     // var dpi_var_array: []*cc.dpiVar = try self.allocator.alloc(*cc.dpiVar, self.column_count);

//     // var dpi_oracle_type_number_array: []cc.dpiOracleTypeNum = try self.allocator.alloc(cc.dpiOracleTypeNum, self.column_count);
//     // var dpi_native_type_number_array: []cc.dpiNativeTypeNum = try self.allocator.alloc(cc.dpiNativeTypeNum, self.column_count);

//     // const ref = records[0];
//     // for (0..self.column_count) |col| {
//     //     switch (ref[col]) {
//     //         .String => |v| {
//     //             dpi_oracle_type_number_array[col] = cc.DPI_ORACLE_TYPE_VARCHAR;
//     //             dpi_native_type_number_array[col] = if (v) |_| cc.DPI_NATIVE_TYPE_BYTES else cc.DPI_NATIVE_TYPE_NULL;
//     //         },
//     //         .Double => |v| {
//     //             dpi_oracle_type_number_array[col] = cc.DPI_ORACLE_TYPE_NUMBER;
//     //             // dpi_native_type_number_array[col] = cc.DPI_NATIVE_TYPE_DOUBLE;
//     //             dpi_native_type_number_array[col] = if (v) |_| cc.DPI_NATIVE_TYPE_DOUBLE else cc.DPI_NATIVE_TYPE_NULL;
//     //         },
//     //         .Int => |v| {
//     //             dpi_oracle_type_number_array[col] = cc.DPI_ORACLE_TYPE_NUMBER;
//     //             dpi_native_type_number_array[col] = if (v) |_| cc.DPI_NATIVE_TYPE_INT64 else cc.DPI_NATIVE_TYPE_NULL;
//     //         },
//     //         .TimeStamp => |v| {
//     //             dpi_oracle_type_number_array[col] = cc.DPI_ORACLE_TYPE_TIMESTAMP;
//     //             dpi_native_type_number_array[col] = if (v) |_| cc.DPI_NATIVE_TYPE_TIMESTAMP else cc.DPI_NATIVE_TYPE_NULL;
//     //         },
//     //         .Boolean => |v| {
//     //             dpi_oracle_type_number_array[col] = cc.DPI_ORACLE_TYPE_NUMBER;
//     //             dpi_native_type_number_array[col] = if (v) |_| cc.DPI_NATIVE_TYPE_INT64 else cc.DPI_NATIVE_TYPE_NULL;
//     //         },
//     //         .Number => |v| {
//     //             dpi_oracle_type_number_array[col] = cc.DPI_ORACLE_TYPE_NUMBER;
//     //             dpi_native_type_number_array[col] = if (v) |_| cc.DPI_NATIVE_TYPE_DOUBLE else cc.DPI_NATIVE_TYPE_NULL;
//     //         },
//     //         else => unreachable,
//     //     }
//     // }

//     for (0..self.column_count) |col| {
//     //      if (cc.dpiConn_newVar(
//     //     conn,
//     //     cc.DPI_ORACLE_TYPE_VARCHAR,
//     //     cc.DPI_NATIVE_TYPE_BYTES,
//     //     batch_size,
//     //     100,
//     //     0,
//     //     0,
//     //     null,
//     //     &name_var,
//     //     null,
//     // ) < 0) {
//     //     try checkError(context, "creating name variable");
//     // }

//         if ()

//         if (cc.dpiConn_newVar(
//             self.conn.conn,
//             dpi_oracle_type_number_array[col],
//             dpi_native_type_number_array[col],
//             @intCast(records.len),
//             0,
//             0,
//             0,
//             null,
//             &dpi_var_array[col],
//             &dpi_val_array[col],
//         ) < 0) {
//             debug.print("Failed to create var with error: {s}\n", .{self.conn.getErrorMessage()});
//             return StatementError.BindStatementError;
//         }
//         if (cc.dpiStmt_bindByPos(self.stmt, @as(u32, @intCast(col)) + 1, dpi_var_array[col]) < 0) {
//             debug.print("Failed to bind var with error: {s}\n", .{self.conn.getErrorMessage()});
//             return StatementError.BindStatementError;
//         }
//     }

//     const columnar: [][]*cc.dpiData = try self.allocator.alloc([]*cc.dpiData, self.column_count);
//     for (columnar) |*col| {
//         col.* = try self.allocator.alloc(*cc.dpiData, records.len);
//     }
//     for (0..self.column_count) |col| {
//         for (0..records.len) |rec| {
//             const ptr = columnar[col][rec];
//             switch (records[rec][col]) {
//                 .String => |v| {
//                     if (v) |val| {
//                         cc.dpiData_setBytes(ptr, val.ptr, @intCast(val.len));
//                     } else {
//                         cc.dpiData_setNull(ptr);
//                     }
//                 },
//                 .Double => |v| {
//                     if (v) |val| {
//                         cc.dpiData_setDouble(ptr, val);
//                     } else {
//                         cc.dpiData_setNull(ptr);
//                     }
//                 },
//                 .Int => |v| {
//                     if (v) |val| {
//                         cc.dpiData_setInt64(ptr, val);
//                     } else {
//                         cc.dpiData_setNull(ptr);
//                     }
//                 },
//                 .Number => |v| {
//                     if (v) |val| {
//                         cc.dpiData_setDouble(ptr, val);
//                     } else {
//                         cc.dpiData_setNull(ptr);
//                     }
//                 },
//                 .Boolean => |v| {
//                     if (v) |val| {
//                         cc.dpiData_setBool(ptr, if (val) 1 else 0);
//                     } else {
//                         cc.dpiData_setNull(ptr);
//                     }
//                 },
//                 .TimeStamp => |v| {
//                     if (v) |val| {
//                         cc.dpiData_setTimestamp(
//                             ptr,
//                             @intCast(val.year),
//                             @intCast(val.month),
//                             @intCast(val.day),
//                             @intCast(val.hour),
//                             @intCast(val.minute),
//                             @intCast(val.second),
//                             0,
//                             0,
//                             0,
//                         );
//                     } else {
//                         cc.dpiData_setNull(ptr);
//                     }
//                 },
//                 else => {
//                     debug.print("NotImplemented column type. col: {d}\n", .{col});
//                     return StatementError.BindStatementError;
//                 },
//             }
//         }
//     }

//     for (0..self.column_count) |col| {
//         if (cc.dpiStmt_bindByPos(self.stmt, @as(u32, @intCast(col)) + 1, columnar[col]) < 0) {
//             debug.print("Failed to bind column {d} with error: {s}\n", .{ col, self.conn.getErrorMessage() });
//             return StatementError.BindStatementError;
//         }
//     }

//     if (cc.dpiStmt_executeMany(self.stmt, cc.DPI_MODE_EXEC_DEFAULT, @intCast(records.len)) < 0) {
//         debug.print("Failed to execute bulk insert with error: {s}\n", .{self.conn.getErrorMessage()});
//         return StatementError.ExecuteStatementError;
//     }
// }

// pub fn batchInsert(self: *Self, records: []Record) !void {
//     const columnar: [][]*cc.dpiData = try self.allocator.alloc([]*cc.dpiData, self.column_count);
//     for (columnar) |*col| {
//         col.* = try self.allocator.alloc(*cc.dpiData, records.len);
//     }
//     for (0..self.column_count) |col| {
//         for (0..records.len) |rec| {
//             const ptr = columnar[col][rec];
//             switch (records[rec][col]) {
//                 .String => |v| cc.dpiData_setBytes(ptr, v.ptr, @intCast(v.len)),
//                 .Double => |v| cc.dpiData_setDouble(ptr, v),
//                 .Int => |v| cc.dpiData_setInt64(ptr, v),
//                 .Number => |v| cc.dpiData_setDouble(ptr, v),
//                 .Boolean => |v| cc.dpiData_setBool(ptr, if (v) 1 else 0),
//                 .TimeStamp => |v| {
//                     cc.dpiData_setTimestamp(
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
//                 .Nil => cc.dpiData_setNull(ptr),
//                 else => {
//                     debug.print("NotImplemented column type. col: {d}\n", .{col});
//                     return StatementError.BindStatementError;
//                 },
//             }
//         }
//     }

//     for (0..self.column_count) |col| {
//         if (cc.dpiStmt_bindByPos(self.stmt, @as(u32, @intCast(col)) + 1, columnar[col]) < 0) {
//             debug.print("Failed to bind column {d} with error: {s}\n", .{ col, self.conn.getErrorMessage() });
//             return StatementError.BindStatementError;
//         }
//     }

//     if (cc.dpiStmt_executeMany(self.stmt, cc.DPI_MODE_EXEC_DEFAULT, @intCast(records.len)) < 0) {
//         debug.print("Failed to execute bulk insert with error: {s}\n", .{self.conn.getErrorMessage()});
//         return StatementError.ExecuteStatementError;
//     }
// }

pub fn fetch(self: *Self) !?Record {
    var buffer_row_index: u32 = 0;
    var native_type_num: cc.dpiNativeTypeNum = 0;

    if (cc.dpiStmt_fetch(self.stmt, &self.found, &buffer_row_index) < 0) {
        debug.print("Failed to fetch rows with error: {s}\n", .{self.conn.getErrorMessage()});
        return StatementError.FetchStatementError;
    }
    if (self.found == 0) {
        return null;
    }
    var row: Record = try self.allocator.alloc(FieldValue, self.column_count);

    for (1..self.column_count + 1) |i| {
        var data: ?*cc.dpiData = undefined;
        if (cc.dpiStmt_getQueryValue(self.stmt, @intCast(i), &native_type_num, &data) < 0) {
            std.debug.print("Failed to get query value with error: {s}\n", .{self.conn.getErrorMessage()});
            return error.FetchStatementError;
        }

        var value: FieldValue = undefined;

        if (data.?.isNull == 0) {
            switch (native_type_num) {
                cc.DPI_NATIVE_TYPE_BYTES => {
                    value = .{
                        .String = try self.allocator.dupe(u8, data.?.value.asBytes.ptr[0..data.?.value.asBytes.length]),
                    };
                },
                cc.DPI_NATIVE_TYPE_FLOAT, cc.DPI_NATIVE_TYPE_DOUBLE => {
                    value = .{ .Double = data.?.value.asDouble };
                },
                cc.DPI_NATIVE_TYPE_INT64 => {
                    value = .{ .Int = data.?.value.asInt64 };
                },
                cc.DPI_NATIVE_TYPE_BOOLEAN => {
                    value = .{ .Boolean = data.?.value.asBoolean > 0 };
                },
                cc.DPI_NATIVE_TYPE_TIMESTAMP => {
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
    _ = cc.dpiStmt_release(self.stmt);
}

test "batchInsert" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var conn = t.getTestConnection(allocator) catch unreachable;
    conn.execute("drop table test_table") catch |err| {
        _ = std.mem.indexOf(u8, conn.getErrorMessage(), "ORA-00942") orelse return err;
    };
    try conn.execute("create table test_table (id number, name varchar2(255))");

    var dpi_var_array: []?*cc.dpiVar = try allocator.alloc(?*cc.dpiVar, 2);
    var id_data: [*c]cc.dpiData = undefined;
    var name_data: [*c]cc.dpiData = undefined;

    // Create variable for IDs
    if (cc.dpiConn_newVar(
        conn.conn,
        cc.DPI_ORACLE_TYPE_NUMBER,
        cc.DPI_NATIVE_TYPE_INT64,
        2,
        0,
        0,
        0,
        null,
        &dpi_var_array[0],
        &id_data,
    ) < 0) {
        std.debug.print("Failed to create variable with error: {s}\n", .{conn.getErrorMessage()});
        unreachable;
    }
    defer {
        if (dpi_var_array[0]) |var_| _ = cc.dpiVar_release(var_);
    }

    // Create variable for names
    if (cc.dpiConn_newVar(
        conn.conn,
        cc.DPI_ORACLE_TYPE_VARCHAR,
        cc.DPI_NATIVE_TYPE_BYTES,
        2,
        5,
        0,
        0,
        null,
        &dpi_var_array[1],
        &name_data,
    ) < 0) {
        std.debug.print("Failed to create variable with error: {s}\n", .{conn.getErrorMessage()});
        unreachable;
    }
    defer {
        if (dpi_var_array[1]) |var_| _ = cc.dpiVar_release(var_);
    }

    const insert_sql = "insert into test_table (id, name) values (:1, :2)";
    const stmt = try conn.prepareStatement(insert_sql);
    for (0..2) |i| {
        if (cc.dpiStmt_bindByPos(stmt.stmt, @as(u32, @intCast(i)) + 1, dpi_var_array[i]) < 0) {
            unreachable;
        }
    }

    // Set first row
    id_data[0].isNull = 0;
    id_data[0].value.asInt64 = 1;

    const n1 = "name1";
    if (cc.dpiVar_setFromBytes(dpi_var_array[1].?, 0, n1.ptr, 5) < 0) {
        std.debug.print("Failed to setFromBytes with error: {s}\n", .{conn.getErrorMessage()});
        unreachable;
    }

    // Set second row
    id_data[1].isNull = 0;
    id_data[1].value.asInt64 = 2;

    const n2 = "name2";
    if (cc.dpiVar_setFromBytes(dpi_var_array[1].?, 1, n2.ptr, 5) < 0) {
        std.debug.print("Failed to setFromBytes with error: {s}\n", .{conn.getErrorMessage()});
        unreachable;
    }

    if (cc.dpiStmt_executeMany(stmt.stmt, cc.DPI_MODE_EXEC_DEFAULT, 2) < 0) {
        unreachable;
    }

    conn.commit() catch unreachable;

    // if (cc.dpiVar_setFromInt64(id_var, @intCast(i), ids[i]) < 0) {
    //         try checkError(context, "setting id value");
    //     }
    //     if (cc.dpiVar_setFromBytes(name_var, @intCast(i), names[i].ptr, names[i].len) < 0) {
    //         try checkError(context, "setting name value");
    //     }

    //     for (i = 0; i < NUM_ROWS; i++) {
    //     intColValue[i].isNull = 0;
    //     intColValue[i].value.asInt64 = gc_IntColValues[i];
    //     if (dpiVar_setFromBytes(stringColVar, i, gc_StringColValues[i],
    //                             strlen(gc_StringColValues[i])) < 0)
    //       return dpiSamples_showError();
    //   }

    // const insert_sql = "insert into test_table (id, name) values (:1, :2)";
    // var dpi_val_array: []*cc.dpiData = try self.allocator.alloc(*cc.dpiData, self.column_count);

    // const sql = "insert into test_table (id, name) values (:1, :2)";
    // var stmt = try conn.prepareStatement(sql);
    // try stmt.execute();
    // var records = allocator.alloc(Record, 2) catch unreachable;
    // records[0][0] = FieldValue{ .Double = 1 };
    // records[0][1] = FieldValue{ .String = @constCast("hello") };
    // records[1][0] = FieldValue{ .Double = 2 };
    // records[1][1] = FieldValue{ .String = @constCast("world") };

    // try stmt.batchInsert(records);
    // try conn.commit();
    // stmt.release();

    // const sql2 = "select * from test_table order by id";
    // stmt = try conn.prepareStatement(sql2);
    // try stmt.execute();
    // var row = try stmt.fetch() orelse unreachable;
    // try testing.expectEqual(row.len, 2);
    // try testing.expectEqual(row[0].Double, 1);
    // try testing.expectEqualStrings(row[1].String, "hello");
    // row = try stmt.fetch() orelse unreachable;
    // try testing.expectEqual(row.len, 2);
    // try testing.expectEqual(row[0].Double, 2);
    // try testing.expectEqualStrings(row[1].String, "world");
    // stmt.release();

    // conn.execute("drop table test_table") catch |err| {
    //     _ = std.mem.indexOf(u8, conn.getErrorMessage(), "ORA-00942") orelse return err;
    // };
}

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
