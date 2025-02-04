const std = @import("std");
const e = @import("../error.zig");
const c = @import("../c.zig").c;
const Connection = @import("../Connection.zig");
const ConnectionOptions = @import("../options.zig").ConnectionOptions;
pub const ConnectionParams = @import("./ConnectionParams.zig");

pub fn connectionParams(allocator: std.mem.Allocator) ConnectionParams {
    return ConnectionParams.initFromEnv(allocator) catch unreachable;
}
pub fn connection(allocator: std.mem.Allocator) Connection {
    return connectionParams(allocator).toConnection();
}

pub fn connectionOptions(allocator: std.mem.Allocator) ConnectionOptions {
    const tp = connectionParams(allocator);
    return .{
        .host = tp.host,
        .database = tp.database,
        .username = tp.username,
        .password = tp.password,
    };
}

pub fn isTableExists(allocator: std.mem.Allocator, conn: *Connection, table_name: []const u8) !bool {
    const sql = try std.fmt.allocPrintZ(
        allocator,
        \\select 1 as one from information_schema.tables where upper(table_name) = upper('{s}')
        \\and upper(table_schema) = upper('public')
        \\and table_catalog = ('{s}')
    ,
        .{ table_name, conn.database },
    );

    defer allocator.free(sql);

    std.debug.assert(conn.pg_conn != null);
    const res = c.PQexec(conn.pg_conn, @ptrCast(sql.ptr));
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        const error_msg = std.mem.span(c.PQresultErrorMessage(res));
        std.debug.print("Error executing sql: {s}\n", .{error_msg});
        std.debug.print("SQL: {s}\n", .{sql});
        return error.Fail;
    }
    const count = c.PQntuples(res);
    return count > 0;
}

pub fn dropTable(allocator: std.mem.Allocator, conn: *Connection, table_name: []const u8) !void {
    const sql = try std.fmt.allocPrintZ(allocator, "DROP TABLE {s}", .{table_name});
    defer allocator.free(sql);
    std.debug.assert(conn.pg_conn != null);
    const res = c.PQexec(conn.pg_conn, @ptrCast(sql.ptr));
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        const error_msg = std.mem.span(c.PQresultErrorMessage(res));
        std.debug.print("Error executing sql: {s}\n", .{error_msg});
        std.debug.print("SQL: {s}\n", .{sql});
        return error.Fail;
    }
}

pub fn createTable(conn: *Connection, sql: []const u8) !void {
    std.debug.assert(conn.pg_conn != null);
    const res = c.PQexec(conn.pg_conn, @ptrCast(sql.ptr));
    defer c.PQclear(res);
    if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
        const error_msg = std.mem.span(c.PQresultErrorMessage(res));
        std.debug.print("Error executing sql: {s}\n", .{error_msg});
        std.debug.print("SQL: {s}\n", .{sql});
        return error.Fail;
    }
}
