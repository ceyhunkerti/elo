const TestTable = @This();

const std = @import("std");
const md = @import("../../shared/db/metadata/metadata.zig");
const utils = @import("../utils.zig");
const Connection = @import("../Connection.zig");

pub const TABLE_NAME = "TEST_TABLE";
pub const CREATE_SCRIPT =
    \\CREATE TABLE {name} (
    \\  ID NUMBER(10) not null,
    \\  NAME VARCHAR2(50) not null,
    \\  AGE NUMBER(10) not null,
    \\  BIRTH_DATE DATE not null,
    \\  IS_ACTIVE NUMBER(1) not null
    \\)
;

allocator: std.mem.Allocator,
conn: *Connection,
table: md.TableName = undefined,
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
        .table = md.TableName.init(allocator, table_name orelse TABLE_NAME, conn.username) catch unreachable,
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
    const exists = try self.isExists();
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
    const exists = try self.isExists();
    if (!exists) return;
    const sql = try std.fmt.allocPrint(self.allocator, "DROP TABLE {s}", .{self.table.name});
    defer self.allocator.free(sql);
    _ = try self.conn.execute(sql);
}

pub fn isExists(self: TestTable) !bool {
    const sql = try std.fmt.allocPrint(self.allocator, "select 1 from {s} where rownum = 1", .{self.name()});
    defer self.allocator.free(sql);
    _ = self.conn.execute(sql) catch |err| {
        if (std.mem.indexOf(u8, self.conn.errorMessage(), "ORA-00942")) |_| {
            return false;
        }
        return err;
    };
    return true;
}
