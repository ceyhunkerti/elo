const std = @import("std");
const shared = @import("../shared.zig");

pub const TABLE_NAME = "TEST_TABLE";
pub const CREATE_SCRIPT =
    \\CREATE TABLE {name} (
    \\  ID INTEGER not null,
    \\  NAME TEXT not null,
    \\)
;

pub fn TestTableType(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        conn: *T,
        table: shared.TableName,
        create_script: []const u8,

        const Self = @This();

        pub fn init(
            allocator: std.mem.Allocator,
            conn: *T,
            table_name: ?[]const u8,
            create_script: ?[]const u8,
        ) !Self {
            return Self{
                .allocator = allocator,
                .conn = conn,
                .table = try shared.TableName.init(allocator, table_name orelse TABLE_NAME, conn.username),
                .create_script = create_script orelse CREATE_SCRIPT,
            };
        }

        pub fn deinit(self: *Self) void {
            self.table.deinit();
        }
        pub fn name(self: TestTable) []const u8 {
            return self.table.name;
        }
        pub fn schema(self: TestTable) []const u8 {
            return self.table.schema;
        }
        pub fn tablename(self: TestTable) []const u8 {
            return self.table.tablename;
        }
    };
}

pub fn TestTable(
    allocator: std.mem.Allocator,
    conn: anytype,
    table_name: ?[]const u8,
    create_script: ?[]const u8,
) !TestTableType(@TypeOf(conn.*)) {
    return TestTableType(@TypeOf(conn.*)).init(
        allocator,
        conn,
        table_name,
        create_script,
    );
}

pub fn deinit(self: TestTable) void {
    self.table.deinit();
}

pub fn name(self: TestTable) []const u8 {
    return self.table.name;
}
pub fn schema(self: TestTable) []const u8 {
    return self.table.schema;
}
pub fn tablename(self: TestTable) []const u8 {
    return self.table.tablename;
}
