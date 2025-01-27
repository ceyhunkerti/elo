const CursorMetadata = @This();

const std = @import("std");
const Column = @import("Column.zig");
const c = @import("../c.zig").c;

allocator: std.mem.Allocator,
cursor_name: []const u8,
columns: []Column,

pub fn init(allocator: std.mem.Allocator, cursor_name: []const u8, res: *const c.PGresult) !CursorMetadata {
    const column_count: u32 = @intCast(c.PQnfields(res));
    const columns = try allocator.alloc(Column, column_count);
    for (columns, 0..) |*column, i| {
        column.* = try Column.fromPGMetadata(allocator, res, @intCast(i));
    }
    return .{
        .allocator = allocator,
        .columns = columns,
        .cursor_name = try allocator.dupe(u8, cursor_name),
    };
}

pub fn deinit(self: CursorMetadata) void {
    self.allocator.free(self.cursor_name);
    for (self.columns) |*column| {
        column.deinit();
    }
    self.allocator.free(self.columns);
}
