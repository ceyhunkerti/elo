const std = @import("std");
const Column = @import("column.zig").Column;

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

pub fn Table(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        name: TableName,
        columns: []Column(T),

        pub fn deinit(self: Table(T)) void {
            self.name.deinit();
            for (self.columns) |column| {
                column.deinit();
            }
            self.allocator.free(self.columns);
        }

        pub fn columnNames(self: Table(T)) ![][]const u8 {
            var names = std.ArrayList([]const u8).init(self.allocator);
            for (self.columns) |column| {
                try names.append(column.name);
            }
            return names.toOwnedSlice();
        }

        pub fn columnCount(self: Table(T)) u32 {
            return @intCast(self.columns.len);
        }
    };
}
