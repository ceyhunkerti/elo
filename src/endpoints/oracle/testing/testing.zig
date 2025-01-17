const std = @import("std");
const connection = @import("connection.zig");
pub const getTestConnection = connection.getTestConnection;
pub const getTestConnectionParams = connection.getTestConnectionParams;
const utils = @import("../utils.zig");
const Connection = @import("../Connection.zig");

pub fn schema() []const u8 {
    const p = getTestConnectionParams() catch unreachable;
    return p.username;
}

pub fn createTestTable(allocator: std.mem.Allocator, conn: *Connection, args: ?struct { schema_dot_table: ?[]const u8, create_script: ?[]const u8 }) !void {
    errdefer {
        std.debug.print("Error: {s}\n", .{conn.errorMessage()});
    }

    if (args) |a| {
        var schema_dot_table: []const u8 = undefined;
        var create_script: []const u8 = undefined;
        if (a.schema_dot_table) |sdt| {
            schema_dot_table = sdt;
        } else {
            schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{schema()});
            defer allocator.free(schema_dot_table);
        }
        if (a.create_script) |script| {
            create_script = script;
        } else {
            create_script = try std.fmt.allocPrint(allocator,
                \\CREATE TABLE {s} (
                \\  ID NUMBER(10) NOT NULL,
                \\  NAME VARCHAR2(50) NOT NULL,
                \\  AGE NUMBER(3) NOT NULL,
                \\  BIRTH_DATE DATE NOT NULL,
                \\  IS_ACTIVE NUMBER(1) NOT NULL
                \\)
            , .{schema_dot_table});
            defer allocator.free(create_script);
        }
        try utils.dropTableIfExists(conn, schema_dot_table);
        _ = try conn.execute(create_script);
        return;
    }

    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{schema()});
    allocator.free(schema_dot_table);
    const create_script = try std.fmt.allocPrint(allocator,
        \\CREATE TABLE {s} (
        \\  ID NUMBER(10) NOT NULL,
        \\  NAME VARCHAR2(50) NOT NULL,
        \\  AGE NUMBER(3) NOT NULL,
        \\  BIRTH_DATE DATE NOT NULL,
        \\  IS_ACTIVE NUMBER(1) NOT NULL
        \\)
    , .{schema_dot_table});
    defer allocator.free(create_script);

    try utils.dropTableIfExists(conn, schema_dot_table);
    _ = try conn.execute(create_script);
}

pub fn isTestTableExist(
    allocator: std.mem.Allocator,
    conn: *Connection,
    args: ?struct { schema_dot_table: ?[]const u8 },
) !bool {
    if (args) |a| {
        var schema_dot_table: []const u8 = undefined;
        if (a.schema_dot_table) |sdt| {
            schema_dot_table = sdt;
        } else {
            schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{schema()});
            defer allocator.free(schema_dot_table);
        }
        return utils.isTableExist(conn, schema_dot_table);
    }

    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{schema()});
    defer allocator.free(schema_dot_table);
    return utils.isTableExist(conn, schema_dot_table);
}

pub fn createTestTableIfNotExists(
    allocator: std.mem.Allocator,
    conn: *Connection,
    args: ?struct { schema_dot_table: ?[]const u8, create_script: ?[]const u8 },
) !void {
    if (try isTestTableExist(allocator, conn, if (args) |a| .{ .schema_dot_table = a.schema_dot_table } else null)) {
        return;
    }
    try createTestTable(allocator, conn, if (args) |a| .{
        .schema_dot_table = a.schema_dot_table,
        .create_script = a.create_script,
    } else null);
}

pub fn dropTestTableIfExist(conn: *Connection, args: ?struct { schema_dot_table: ?[]const u8 }) !void {
    if (args) |a| {
        var schema_dot_table: []const u8 = undefined;
        if (a.schema_dot_table) |sdt| {
            schema_dot_table = sdt;
        } else {
            schema_dot_table = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.TEST_TABLE", .{schema()});
        }
        try utils.dropTableIfExists(conn, schema_dot_table);
        return;
    }
}
