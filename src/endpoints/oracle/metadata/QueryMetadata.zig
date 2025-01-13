const std = @import("std");
const Allocator = std.mem.Allocator;
const Statement = @import("../Statement.zig");
const Column = @import("./Column.zig");

const Self = @This();

allocator: Allocator,
stmt: *Statement,
columns: []Column,

pub fn init(allocator: Allocator, stmt: *Statement) !Self {
    const md = .{
        .allocator = allocator,
        .stmt = stmt,
        .columns = try allocator.alloc(Column, stmt.column_count),
    };

    for (md.columns, 1..) |*column, i| {
        column.* = try Column.fromStatement(allocator, stmt, @intCast(i));
    }
    return md;
}

pub fn columnNames(self: Self) ![]const []const u8 {
    var names = try self.allocator.alloc([]const u8, self.columns.len);
    for (self.columns, 0..) |column, i| {
        names[i] = self.allocator.dupe(u8, column.name) catch unreachable;
    }
    return names;
}

pub fn columnCount(self: Self) u16 {
    return @intCast(self.columns.len);
}
