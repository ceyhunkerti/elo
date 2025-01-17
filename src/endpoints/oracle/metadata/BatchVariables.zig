const std = @import("std");
const Column = @import("Column.zig");
const Record = @import("../../../commons.zig").Record;
const Allocator = std.mem.Allocator;
const c = @import("../c.zig").c;
const Self = @This();
const checkError = @import("../utils.zig").checkError;
const Connector = @import("../Connector.zig");
const Statement = @import("../Statement.zig");

allocator: Allocator = undefined,
sql: []const u8 = "",
batch_size: u32 = 10_000,
columns: []const Column = undefined,
dpi_vars: []?*c.dpiVar = undefined,
dpi_data_array: []?[*c]c.dpiData = undefined,

pub fn init(allocator: Allocator, sql: []const u8, record: Record) !Self {
    const columns = try allocator.alloc(Column, record.len);
    for (record, 0..) |val, i| {
        columns[i] = try Column.fromFieldValue(allocator, val);
    }
    return .{
        .allocator = allocator,
        .sql = sql,
        .columns = columns,
        .dpi_vars = try allocator.alloc(?*c.dpiVar, columns.len),
        .dpi_data_array = try allocator.alloc(?[*c]c.dpiData, columns.len),
    };
}

pub fn bindAll(self: *Self, statement: *Statement) !void {
    for (0..self.columns.len) |i| {
        try checkError(
            c.dpiStmt_bindByPos(statement.dpi_stmt, @as(u32, @intCast(i)) + 1, self.dpi_vars[i]),
            error.BindStatementError,
        );
    }
}

pub fn initDpiVarArray(self: *Self, connector: Connector) void {
    for (self.columns, 0..) |column, i| {
        if (c.dpiConn_newVar(
            connector.dpi_conn,
            column.oracle_type_num,
            column.native_type_num,
            self.batch_size,
            column.size,
            0,
            0,
            null,
            &self.dpi_vars[i],
            &self.dpi_data_array[i].?,
        ) < 0) {
            std.debug.print("Failed to create variable with error: {s}\n", .{connector.errorMessage()});
            unreachable;
        }
    }
}

pub fn deinit() void {
    // todo
    // for () {
    //     // _ = c.dpiVar_release(var_);
    // }

    // self.allocator.free(self.dpi_var_array);
    // self.allocator.free(self.dpi_data_array);
    // self.allocator.free(self.columns);
}
