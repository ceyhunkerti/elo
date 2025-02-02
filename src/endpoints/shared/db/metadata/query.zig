const Query = @This();
const std = @import("std");
const Column = @import("Column.zig");

allocator: std.mem.Allocator,
sql: []const u8,
columns: []Column,

pub fn init(allocator: std.mem.Allocator, sql: []const u8, columns: []Column) Query {
    return .{
        .allocator = allocator,
        .sql = sql,
        .columns = columns,
    };
}
