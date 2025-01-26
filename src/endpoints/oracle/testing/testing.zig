const std = @import("std");
const tc = @import("connection.zig");
pub const getTestConnection = tc.getTestConnection;
pub const getTestConnectionParams = tc.getTestConnectionParams;
const utils = @import("../utils.zig");
const Connection = @import("../Connection.zig");

pub const Error = error{MissingTestEnvironmentVariable};

pub const ConnectionParams = struct {
    allocator: std.mem.Allocator,
    username: []const u8,
    password: []const u8,
    connection_string: []const u8,
    privilege: Connection.Privilege,

    pub fn initFromEnv(allocator: std.mem.Allocator) !ConnectionParams {
        const username = std.posix.getenv("ORACLE_TEST_USERNAME") orelse {
            std.debug.print("Missing ORACLE_TEST_USERNAME environment variable\n", .{});
            return error.MissingTestEnvironmentVariable;
        };
        const password = std.posix.getenv("ORACLE_TEST_PASSWORD") orelse {
            std.debug.print("Missing ORACLE_TEST_PASSWORD environment variable\n", .{});
            return error.MissingTestEnvironmentVariable;
        };
        const connection_string = std.posix.getenv("ORACLE_TEST_CONNECTION_STRING") orelse {
            std.debug.print("Missing ORACLE_TEST_CONNECTION_STRING environment variable\n", .{});
            return error.MissingTestEnvironmentVariable;
        };
        const auth_mode = std.posix.getenv("ORACLE_TEST_AUTH_MODE") orelse {
            std.debug.print("Missing ORACLE_TEST_AUTH_MODE environment variable\n", .{});
            return error.MissingTestEnvironmentVariable;
        };

        return ConnectionParams{
            .allocator = allocator,
            .username = username,
            .password = password,
            .connection_string = connection_string,
            .privilege = try Connection.Privilege.fromString(auth_mode),
        };
    }

    pub fn toConnection(self: ConnectionParams) Connection {
        return Connection.init(
            self.allocator,
            self.username,
            self.password,
            self.connection_string,
            self.privilege,
        );
    }
};

pub fn connection(allocator: std.mem.Allocator) Connection {
    const tp = ConnectionParams.initFromEnv(allocator) catch unreachable;
    return tp.toConnection();
}

pub fn schema() []const u8 {
    const p = ConnectionParams.initFromEnv(std.testing.allocator) catch unreachable;
    return p.username;
}

pub fn connectionParams(allocator: std.mem.Allocator) !ConnectionParams {
    return ConnectionParams.initFromEnv(allocator) catch unreachable;
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

    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{schema()});
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
