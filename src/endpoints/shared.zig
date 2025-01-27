const std = @import("std");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const alloc = gpa.allocator();

pub const TableName = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    schema: []const u8,
    tablename: []const u8,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, default_schema: ?[]const u8) !TableName {
        var tn = TableName{
            .allocator = allocator,
            .name = undefined,
            .schema = undefined,
            .tablename = undefined,
        };

        var tokens = std.mem.split(u8, name, ".");
        const schema_or_tablename = tokens.first();
        const tablename = tokens.rest();

        if (tablename.len == 0) {
            tn.schema = try allocator.dupe(u8, default_schema orelse "");
            tn.tablename = try allocator.dupe(u8, schema_or_tablename);
            tn.name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ tn.schema, tn.tablename });
        } else {
            tn.name = try allocator.dupe(u8, name);
            tn.schema = try allocator.dupe(u8, schema_or_tablename);
            tn.tablename = try allocator.dupe(u8, tablename);
        }

        return tn;
    }

    pub fn deinit(self: TableName) void {
        self.allocator.free(self.name);
        self.allocator.free(self.schema);
        self.allocator.free(self.tablename);
    }
};
test "TableName" {
    const allocator = std.testing.allocator;
    const tn1 = try TableName.init(allocator, "DEMO.TEST_TABLE", null);
    defer tn1.deinit();

    try std.testing.expectEqualStrings("DEMO.TEST_TABLE", tn1.name);
    try std.testing.expectEqualStrings("DEMO", tn1.schema);
    try std.testing.expectEqualStrings("TEST_TABLE", tn1.tablename);

    const tn2 = try TableName.init(allocator, "TEST_TABLE", "SYS");
    defer tn2.deinit();

    try std.testing.expectEqualStrings("SYS.TEST_TABLE", tn2.name);
    try std.testing.expectEqualStrings("SYS", tn2.schema);
    try std.testing.expectEqualStrings("TEST_TABLE", tn2.tablename);
}

pub fn truncateTable(conn: anytype, table: []const u8) !void {
    const sql = try std.fmt.allocPrint(alloc, "truncate table {s}", .{table});
    defer alloc.free(sql);
    _ = try conn.execute(sql);
}
