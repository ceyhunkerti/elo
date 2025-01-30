const TestTable = @This();

const std = @import("std");
const shared = @import("../../shared.zig");
const utils = @import("../utils.zig");
const Connection = @import("../Connection.zig");

pub const TABLE_NAME = "TEST_TABLE";
pub const CREATE_SCRIPT =
    \\CREATE TABLE {name} (
    \\  ID INT not null,
    \\  NAME  TEXT not null,
    \\  AGE INT not null,
    \\  BIRTH_DATE DATE not null,
    \\  IS_ACTIVE INT not null
    \\)
;

allocator: std.mem.Allocator,
conn: *Connection,
table: shared.TableName = undefined,
create_script: []const u8 = undefined,

pub fn init(
    allocator: std.mem.Allocator,
    conn: *Connection,
    table_name: ?[]const u8,
    create_script: ?[]const u8,
) TestTable {
    return .{
        .allocator = allocator,
        .conn = conn,
        .table = shared.TableName.init(allocator, table_name orelse TABLE_NAME, conn.username) catch unreachable,
        .create_script = create_script orelse CREATE_SCRIPT,
    };
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

pub fn createIfNotExists(self: TestTable) !void {
    const exists = try utils.isTableExist(self.conn, self.table.name);
    if (exists) return;
    const create_script = try self.resolveCreateScript();
    defer self.allocator.free(create_script);
    _ = try self.conn.execute(create_script);
}

pub fn resolveCreateScript(self: TestTable) ![]const u8 {
    const create_script = try self.allocator.alloc(u8, 4096); // 4096 is arbitrary
    @memset(create_script, 0);
    _ = std.mem.replace(u8, self.create_script, "{name}", self.table.name, create_script);

    return create_script;
}

pub fn dropIfExists(self: TestTable) !void {
    const exists = try utils.isTableExist(self.conn, self.table.name);
    if (!exists) return;
    const sql = try std.fmt.allocPrint(self.allocator, "DROP TABLE {s}", .{self.table.name});
    defer self.allocator.free(sql);
    _ = try self.conn.execute(sql);
}
