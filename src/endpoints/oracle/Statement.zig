const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @import("c.zig").c;
const Connection = @import("Connection.zig");
const commons = @import("../../commons.zig");
const Record = commons.Record;
const FieldValue = commons.FieldValue;

const checkError = @import("./utils.zig").checkError;
const Self = @This();

allocator: Allocator = undefined,
connection: *Connection = null,
dpi_stmt: ?*c.dpiStmt = null,

pub const StatementError = error{
    StatementConfigError,
    ExecuteStatementError,
    FetchStatementError,
};

pub const BatchInsertPayload = struct {
    dpi_vars: []?*c.dpiVar = undefined,
    dpi_data_array: []?[*c]c.dpiData = undefined,
};

pub fn init(allocator: Allocator, connection: *Connection) Self {
    return .{
        .allocator = allocator,
        .connection = connection,
    };
}

pub fn prepare(self: *Self, sql: []const u8) !void {
    try checkError(
        c.dpiConn_prepareStmt(self.connection.dpi_conn, 0, sql.ptr, @intCast(sql.len), null, 0, &self.dpi_stmt),
        error.PrepareStatementError,
    );
}

pub fn release(self: *Self) void {
    _ = c.dpiStmt_release(self.dpi_stmt);
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
        return StatementError.FetchStatementError;
    }
    if (found == 0) {
        return null;
    }
    var row: Record = try self.allocator.alloc(FieldValue, column_count);

    for (1..column_count + 1) |i| {
        var data: ?*c.dpiData = undefined;
        if (c.dpiStmt_getQueryValue(self.dpi_stmt, @intCast(i), &native_type_num, &data) < 0) {
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
                    return error.FetchStatementError;
                },
            }
        }
        row[i - 1] = value;
    }

    return row;
}

pub fn executeMany(self: *Self, num_iters: u32) !void {
    try checkError(
        c.dpiStmt_executeMany(self.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, num_iters),
        error.ExecuteStatementError,
    );
}

// const std = @import("std");
// const debug = std.debug;
// const testing = std.testing;
// const Allocator = std.mem.Allocator;
// const commons = @import("../../commons.zig");
// const Record = commons.Record;
// const FieldValue = commons.FieldValue;
// const c = @import("c.zig").c;
// const t = @import("testing/testing.zig");
// const utils = @import("./utils.zig");
// const checkError = utils.checkError;

// const InsertMetadata = @import("./metadata/InsertMetadata.zig");
// const Connector = @import("Connector.zig");

// const Self = @This();
// const StatementError = error{
//     PrepareStatementError,
//     ExecuteStatementError,
//     FetchStatementError,
//     StatementConfigError,
//     BindStatementError,
// };

// allocator: Allocator,
// connector: *Connector = undefined,
// dpi_stmt: ?*c.dpiStmt = null,
// column_count: u32 = 0,
// sql: []const u8 = "",
// found: c_int = 0,
// fetch_size: u32 = c.DPI_DEFAULT_FETCH_ARRAY_SIZE,

// pub fn init(allocator: Allocator, connector: *Connector) Self {
//     return .{
//         .allocator = allocator,
//         .connector = connector,
//     };
// }

// pub fn setInsertMetadata(self: *Self, insert_metadata: InsertMetadata) void {
//     self.insert_metadata = insert_metadata;
// }

// pub fn setFetchSize(self: *Self, fetch_size: u32) !void {
//     if (fetch_size > 0) {
//         try checkError(
//             c.dpiStmt_setFetchArraySize(self.dpi_stmt, fetch_size),
//             error.StatementConfigError,
//         );
//         self.fetch_size = fetch_size;
//     }
// }

// pub fn prepare(self: *Self, sql: []const u8) !void {
//     self.sql = sql;
//     try checkError(
//         c.dpiConn_prepareStmt(self.connector.dpi_conn, 0, self.sql.ptr, @intCast(self.sql.len), null, 0, &self.dpi_stmt),
//         error.PrepareStatementError,
//     );
// }

// pub fn execute(self: *Self) StatementError!void {
//     try checkError(
//         c.dpiStmt_execute(self.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, &self.column_count),
//         error.ExecuteStatementError,
//     );
// }

// pub fn fetch(self: *Self) !?Record {
//     var buffer_row_index: u32 = 0;
//     var native_type_num: c.dpiNativeTypeNum = 0;

//     if (c.dpiStmt_fetch(self.stmt, &self.found, &buffer_row_index) < 0) {
//         debug.print("Failed to fetch rows with error: {s}\n", .{self.conn.getErrorMessage()});
//         return StatementError.FetchStatementError;
//     }
//     if (self.found == 0) {
//         return null;
//     }
//     var row: Record = try self.allocator.alloc(FieldValue, self.column_count);

//     for (1..self.column_count + 1) |i| {
//         var data: ?*c.dpiData = undefined;
//         if (c.dpiStmt_getQueryValue(self.stmt, @intCast(i), &native_type_num, &data) < 0) {
//             std.debug.print("Failed to get query value with error: {s}\n", .{self.conn.getErrorMessage()});
//             return error.FetchStatementError;
//         }

//         var value: FieldValue = undefined;

//         if (data.?.isNull == 0) {
//             switch (native_type_num) {
//                 c.DPI_NATIVE_TYPE_BYTES => {
//                     value = .{
//                         .String = try self.allocator.dupe(u8, data.?.value.asBytes.ptr[0..data.?.value.asBytes.length]),
//                     };
//                 },
//                 c.DPI_NATIVE_TYPE_FLOAT, c.DPI_NATIVE_TYPE_DOUBLE => {
//                     value = .{ .Double = data.?.value.asDouble };
//                 },
//                 c.DPI_NATIVE_TYPE_INT64 => {
//                     value = .{ .Int = data.?.value.asInt64 };
//                 },
//                 c.DPI_NATIVE_TYPE_BOOLEAN => {
//                     value = .{ .Boolean = data.?.value.asBoolean > 0 };
//                 },
//                 c.DPI_NATIVE_TYPE_TIMESTAMP => {
//                     // todo
//                     const ts = data.?.value.asTimestamp;
//                     value = .{ .TimeStamp = .{
//                         .day = ts.day,
//                         .hour = ts.hour,
//                         .minute = ts.minute,
//                         .month = ts.month,
//                         .second = ts.second,
//                         .year = @intCast(ts.year),
//                     } };
//                 },
//                 else => {
//                     debug.print("Failed to get query value with error: {s}\n", .{self.conn.getErrorMessage()});
//                     return error.FetchStatementError;
//                 },
//             }
//         }
//         row[i - 1] = value;
//     }

//     return row;
// }

// pub fn release(self: *Self) void {
//     _ = c.dpiStmt_release(self.stmt);
// }

// pub fn batchInsert(self: *Self, records: []Record) !void {
//     var dpi_data_array: []?[*c]c.dpiData = try self.allocator.alloc(?[*c]c.dpiData, records.len);
//     defer self.allocator.free(dpi_data_array);

//     for (self.insert_metadata.columns, 0..) |column, i| {
//         if (c.dpiConn_newVar(
//             self.connector.dpi_conn,
//             column.oracle_type_num,
//             column.native_type_num,
//             records.len,
//             0,
//             0,
//             0,
//             null,
//             &dpi_data_array[i],
//             null,
//         ) < 0) {
//             std.debug.print("Failed to create variable with error: {s}\n", .{self.connector.errorMessage()});
//             return error.BindNewVariableError;
//         }
//     }

//     // const column_count = 2;
//     // var dpi_var_array: []?*c.dpiVar = try allocator.alloc(?*c.dpiVar, column_count);
//     // var data_array: []?[*c]c.dpiData = try allocator.alloc(?[*c]c.dpiData, 3);

//     // // Create variable for IDs
//     // if (c.dpiConn_newVar(
//     //     connector.dpi_conn,
//     //     c.DPI_ORACLE_TYPE_NUMBER,
//     //     c.DPI_NATIVE_TYPE_INT64,
//     //     3,
//     //     0,
//     //     0,
//     //     0,
//     //     null,
//     //     &dpi_var_array[0],
//     //     &data_array[0].?,
//     // ) < 0) {
//     //     std.debug.print("Failed to create variable with error: {s}\n", .{connector.errorMessage()});
//     //     unreachable;
//     // }
//     // defer {
//     //     if (dpi_var_array[0]) |var_| _ = c.dpiVar_release(var_);
//     // }

//     // // Create variable for names
//     // if (c.dpiConn_newVar(
//     //     connector.dpi_conn,
//     //     c.DPI_ORACLE_TYPE_VARCHAR,
//     //     c.DPI_NATIVE_TYPE_BYTES,
//     //     3,
//     //     5,
//     //     0,
//     //     0,
//     //     null,
//     //     &dpi_var_array[1],
//     //     &data_array[1].?,
//     // ) < 0) {
//     //     std.debug.print("Failed to create variable with error: {s}\n", .{connector.errorMessage()});
//     //     unreachable;
//     // }
//     // defer {
//     //     if (dpi_var_array[1]) |var_| _ = c.dpiVar_release(var_);
//     // }

//     // const insert_sql = "insert into test_table (idx, name) values (:1, :2)";
//     // const stmt = try connector.prepareStatement(insert_sql);
//     // for (0..column_count) |i| {
//     //     if (c.dpiStmt_bindByPos(stmt.dpi_stmt, @as(u32, @intCast(i)) + 1, dpi_var_array[i]) < 0) {
//     //         unreachable;
//     //     }
//     // }

//     // // Set first row
//     // data_array[0].?[0].isNull = 0;
//     // data_array[0].?[0].value.asInt64 = 1;

//     // const n1 = "name1";
//     // if (c.dpiVar_setFromBytes(dpi_var_array[1].?, 0, n1.ptr, 5) < 0) {
//     //     std.debug.print("Failed to setFromBytes with error: {s}\n", .{connector.errorMessage()});
//     //     unreachable;
//     // }

//     // // Set second row
//     // data_array[0].?[1].isNull = 0;
//     // data_array[0].?[1].value.asInt64 = 2;

//     // const n2 = "name2";
//     // if (c.dpiVar_setFromBytes(dpi_var_array[1].?, 1, n2.ptr, 5) < 0) {
//     //     std.debug.print("Failed to setFromBytes with error: {s}\n", .{connector.errorMessage()});
//     //     unreachable;
//     // }

//     // // Set third row
//     // data_array[0].?[2].isNull = 0;
//     // data_array[0].?[2].value.asInt64 = 3;

//     // const n3 = "name3";
//     // if (c.dpiVar_setFromBytes(dpi_var_array[1].?, 2, n3.ptr, 5) < 0) {
//     //     std.debug.print("Failed to setFromBytes with error: {s}\n", .{connector.errorMessage()});
//     //     unreachable;
//     // }

//     // if (c.dpiStmt_executeMany(stmt.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, 3) < 0) {
//     //     unreachable;
//     // }

//     // try connector.execute("commit");
// }

// test "batchInsert" {
//     var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arena.deinit();
//     const allocator = arena.allocator();

//     var connector = t.getTestConnector(allocator) catch unreachable;
//     try connector.connect();
//     connector.execute("drop table test_table") catch |err| {
//         _ = std.mem.indexOf(u8, connector.errorMessage(), "ORA-00942") orelse return err;
//     };
//     try connector.execute("create table test_table (idx number, name varchar2(255))");

//     const column_count = 2;
//     var dpi_var_array: []?*c.dpiVar = try allocator.alloc(?*c.dpiVar, column_count);
//     var data_array: []?[*c]c.dpiData = try allocator.alloc(?[*c]c.dpiData, 3);

//     std.debug.print("column_count: {}\n", .{column_count});

//     // Create variable for IDs
//     if (c.dpiConn_newVar(
//         connector.dpi_conn,
//         c.DPI_ORACLE_TYPE_NUMBER,
//         c.DPI_NATIVE_TYPE_INT64,
//         5,
//         0,
//         0,
//         0,
//         null,
//         &dpi_var_array[0],
//         &data_array[0].?,
//     ) < 0) {
//         std.debug.print("Failed to create variable with error: {s}\n", .{connector.errorMessage()});
//         unreachable;
//     }
//     defer {
//         if (dpi_var_array[0]) |var_| _ = c.dpiVar_release(var_);
//     }

//     // Create variable for names
//     if (c.dpiConn_newVar(
//         connector.dpi_conn,
//         c.DPI_ORACLE_TYPE_VARCHAR,
//         c.DPI_NATIVE_TYPE_BYTES,
//         5,
//         5,
//         0,
//         0,
//         null,
//         &dpi_var_array[1],
//         &data_array[1].?,
//     ) < 0) {
//         std.debug.print("Failed to create variable with error: {s}\n", .{connector.errorMessage()});
//         unreachable;
//     }
//     defer {
//         if (dpi_var_array[1]) |var_| _ = c.dpiVar_release(var_);
//     }

//     const insert_sql = "insert into test_table (idx, name) values (:1, :2)";
//     const stmt = try connector.prepareStatement(insert_sql);
//     for (0..column_count) |i| {
//         if (c.dpiStmt_bindByPos(stmt.dpi_stmt, @as(u32, @intCast(i)) + 1, dpi_var_array[i]) < 0) {
//             unreachable;
//         }
//     }

//     // Set first row
//     data_array[0].?[0].isNull = 0;
//     data_array[0].?[0].value.asInt64 = 1;

//     const n1 = "name1";
//     if (c.dpiVar_setFromBytes(dpi_var_array[1].?, 0, n1.ptr, 5) < 0) {
//         std.debug.print("Failed to setFromBytes with error: {s}\n", .{connector.errorMessage()});
//         unreachable;
//     }

//     // Set second row
//     data_array[0].?[1].isNull = 0;
//     data_array[0].?[1].value.asInt64 = 2;

//     const n2 = "name2";
//     if (c.dpiVar_setFromBytes(dpi_var_array[1].?, 1, n2.ptr, 5) < 0) {
//         std.debug.print("Failed to setFromBytes with error: {s}\n", .{connector.errorMessage()});
//         unreachable;
//     }

//     // Set third row
//     data_array[0].?[2].isNull = 0;
//     data_array[0].?[2].value.asInt64 = 3;

//     const n3 = "name3";
//     if (c.dpiVar_setFromBytes(dpi_var_array[1].?, 2, n3.ptr, 5) < 0) {
//         std.debug.print("Failed to setFromBytes with error: {s}\n", .{connector.errorMessage()});
//         unreachable;
//     }

//     if (c.dpiStmt_executeMany(stmt.dpi_stmt, c.DPI_MODE_EXEC_DEFAULT, 3) < 0) {
//         unreachable;
//     }

//     try connector.execute("commit");

//     // if (c.dpiVar_setFromInt64(id_var, @intCast(i), ids[i]) < 0) {
//     //         try checkError(context, "setting id value");
//     //     }
//     //     if (c.dpiVar_setFromBytes(name_var, @intCast(i), names[i].ptr, names[i].len) < 0) {
//     //         try checkError(context, "setting name value");
//     //     }

//     //     for (i = 0; i < NUM_ROWS; i++) {
//     //     intColValue[i].isNull = 0;
//     //     intColValue[i].value.asInt64 = gc_IntColValues[i];
//     //     if (dpiVar_setFromBytes(stringColVar, i, gc_StringColValues[i],
//     //                             strlen(gc_StringColValues[i])) < 0)
//     //       return dpiSamples_showError();
//     //   }

//     // const insert_sql = "insert into test_table (id, name) values (:1, :2)";
//     // var dpi_val_array: []*c.dpiData = try self.allocator.alloc(*c.dpiData, self.column_count);

//     // const sql = "insert into test_table (id, name) values (:1, :2)";
//     // var stmt = try conn.prepareStatement(sql);
//     // try stmt.execute();
//     // var records = allocator.alloc(Record, 2) catch unreachable;
//     // records[0][0] = FieldValue{ .Double = 1 };
//     // records[0][1] = FieldValue{ .String = @constCast("hello") };
//     // records[1][0] = FieldValue{ .Double = 2 };
//     // records[1][1] = FieldValue{ .String = @constCast("world") };

//     // try stmt.batchInsert(records);
//     // try conn.commit();
//     // stmt.release();

//     // const sql2 = "select * from test_table order by id";
//     // stmt = try conn.prepareStatement(sql2);
//     // try stmt.execute();
//     // var row = try stmt.fetch() orelse unreachable;
//     // try testing.expectEqual(row.len, 2);
//     // try testing.expectEqual(row[0].Double, 1);
//     // try testing.expectEqualStrings(row[1].String, "hello");
//     // row = try stmt.fetch() orelse unreachable;
//     // try testing.expectEqual(row.len, 2);
//     // try testing.expectEqual(row[0].Double, 2);
//     // try testing.expectEqualStrings(row[1].String, "world");
//     // stmt.release();

//     // conn.execute("drop table test_table") catch |err| {
//     //     _ = std.mem.indexOf(u8, conn.getErrorMessage(), "ORA-00942") orelse return err;
//     // };
// }

// // test "fetch" {
// //     var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
// //     defer arena.deinit();
// //     const sql =
// //         \\select
// //         \\1 as A, 2 as B, 'hello' as C, to_date('2020-01-01', 'yyyy-mm-dd') as D
// //         \\from dual
// //     ;
// //     var conn = t.getTestConnection(arena.allocator()) catch unreachable;

// //     var stmt = try conn.prepareStatement(sql);
// //     try stmt.execute();

// //     var row: Record = undefined;
// //     try stmt.fetch(&row);

// //     try testing.expectEqual(row.len, 4);
// //     try testing.expectEqual(row[0].Double, 1);
// //     try testing.expectEqual(row[1].Double, 2);
// //     try testing.expectEqualStrings(row[2].String, "hello");
// //     try testing.expectEqual(row[3].TimeStamp.day, 1);
// //     try testing.expectEqual(row[3].TimeStamp.month, 1);
// //     try testing.expectEqual(row[3].TimeStamp.year, 2020);
// // }
