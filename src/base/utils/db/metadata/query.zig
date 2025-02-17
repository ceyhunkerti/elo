const Column = @import("column.zig").Column;
const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Query(comptime T: type) type {
    return struct {
        allocator: Allocator,
        count: ?u32 = null,
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

        pub fn chunks(self: Query(T), chunk_count: u16) ?[]u32 {
            if (self.count == null) return null;

            const res = std.ArrayList(u32).init(self.allocator);
            defer res.deinit();

            if (chunk_count >= self.count.?) return &[1]u32{self.count.?};

            const chunk_size = @divTrunc(self.count.?, chunk_count);
            var current_size: u32 = 0;

            while (current_size < chunk_size) : (current_size += chunk_size) {
                try res.append(current_size);
            }
            if (current_size < self.count.?) {
                try res.append(self.count.?);
            }

            return res.toOwnedSlice();
        }
    };
}
