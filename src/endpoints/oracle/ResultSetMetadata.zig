const std = @import("std");
const ResultSet = @import("ResultSet.zig");
const Column = @import("metadata/Column.zig");

const Self = @This();

allocator: std.mem.Allocator,
result_set: *ResultSet,

column_count: u32 = 0,
columns: []Column,

pub fn init(allocator: std.mem.Allocator, result_set: *ResultSet) !Self {
    const column_count = result_set.getColumnCount();
    const columns = try allocator.alloc(Column, column_count);
    for (columns, 1..) |*column, i| {
        column.* = try result_set.getColumn(@intCast(i));
    }

    return Self{
        .allocator = allocator,
        .result_set = result_set,
        .column_count = column_count,
        .columns = columns,
    };
}
pub fn deinit(self: *Self) void {
    for (self.columns) |*column| column.deinit();
    self.allocator.free(self.columns);
}

pub fn getColumn(self: Self, index: u32) Column {
    return self.columns[index - 1];
}
pub fn getColumnName(self: Self, index: u32) []const u8 {
    return self.columns[index - 1].name;
}
pub fn getColumnSqlType(self: Self, index: u32) []const u8 {
    return self.columns[index - 1].sql_type;
}
pub fn getColumnType(self: Self, index: u32) c_uint {
    return self.columns[index - 1].type;
}
pub fn getColumnSubType(self: Self, index: u32) c_uint {
    return self.columns[index - 1].sub_type;
}
