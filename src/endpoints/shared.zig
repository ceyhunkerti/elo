const std = @import("std");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const alloc = gpa.allocator();

pub fn truncateTable(conn: anytype, table: []const u8) !void {
    const sql = try std.fmt.allocPrint(alloc, "truncate table {s}", .{table});
    defer alloc.free(sql);
    _ = try conn.execute(sql);
}

test {
    std.testing.refAllDecls(@This());
}
