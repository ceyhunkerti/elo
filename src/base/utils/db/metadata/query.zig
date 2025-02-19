const Column = @import("column.zig").Column;
const std = @import("std");
const Allocator = std.mem.Allocator;

const Error = error{
    CountIsNotInitialized,
};

pub fn Query(comptime T: type) type {
    return struct {
        allocator: Allocator,
        query: []const u8,
        columns: []Column(T),

        pub fn init(allocator: Allocator, column_count: usize, query: []const u8) !Query(T) {
            return .{
                .allocator = allocator,
                .query = query,
                .columns = try allocator.alloc(Column(T), column_count),
            };
        }

        pub fn deinit(self: Query(T)) void {
            for (self.columns) |column| {
                column.deinit();
            }
            self.allocator.free(self.columns);
        }
    };
}
