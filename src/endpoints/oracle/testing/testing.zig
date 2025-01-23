const std = @import("std");

const utils = @import("../utils.zig");
const Connection = @import("../Connection.zig");
const tu = @import("./testutils.zig");

pub const ConnectionParams = tu.ConnectionParams;
pub const getConnection = tu.getConnection;

pub fn createTestTable(allocator: std.mem.Allocator, conn: *Connection, args: ?struct { schema_dot_table: ?[]const u8, create_script: ?[]const u8 }) !void {
    const cp = try ConnectionParams.init();

    errdefer {
        std.debug.print("Error: {s}\n", .{conn.errorMessage()});
    }

    if (args) |a| {
        var schema_dot_table: []const u8 = undefined;
        var create_script: []const u8 = undefined;
        if (a.schema_dot_table) |sdt| {
            schema_dot_table = sdt;
        } else {
            schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{cp.username});
            defer allocator.free(schema_dot_table);
        }
        if (a.create_script) |script| {
            create_script = script;
        } else {
            create_script = try std.fmt.allocPrint(allocator,
                \\CREATE TABLE {s} (
                \\  ID NUMBER(10) not null,
                \\  NAME VARCHAR2(50) not null,
                \\  AGE NUMBER(10) not null,
                \\  BIRTH_DATE DATE not null,
                \\  IS_ACTIVE NUMBER(1) not null
                \\)
            , .{schema_dot_table});
            defer allocator.free(create_script);
        }
        try utils.dropTableIfExists(conn, schema_dot_table);
        _ = try conn.execute(create_script);
        return;
    }

    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{cp.username});
    defer allocator.free(schema_dot_table);
    const create_script = try std.fmt.allocPrint(allocator,
        \\CREATE TABLE {s} (
        \\  ID NUMBER(10) not null,
        \\  NAME VARCHAR2(50) not null,
        \\  AGE NUMBER(10) not null,
        \\  BIRTH_DATE DATE not null,
        \\  IS_ACTIVE NUMBER(1) not null
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
    const cp = try ConnectionParams.init();

    if (args) |a| {
        var schema_dot_table: []const u8 = undefined;
        if (a.schema_dot_table) |sdt| {
            schema_dot_table = sdt;
        } else {
            schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{cp.username});
            defer allocator.free(schema_dot_table);
        }
        return utils.isTableExist(conn, schema_dot_table);
    }

    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{cp.username});
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
    const cp = try ConnectionParams.init();

    if (args) |a| {
        var schema_dot_table: []const u8 = undefined;
        if (a.schema_dot_table) |sdt| {
            schema_dot_table = sdt;
        } else {
            schema_dot_table = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.TEST_TABLE", .{cp.username});
        }
        try utils.dropTableIfExists(conn, schema_dot_table);
        return;
    }
}
