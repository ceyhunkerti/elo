const std = @import("std");
const Allocator = std.mem.Allocator;
const ConnectionOptions = @import("./options.zig").ConnectionOptions;
const Connection = @import("./Connection.zig");
const c = @import("c.zig").c;

const Error = error{
    TableNotFound,
};

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const a = gpa.allocator();

pub fn initConnection(allocator: Allocator, options: ConnectionOptions) *Connection {
    const connection = allocator.create(Connection) catch unreachable;
    connection.* = .{
        .allocator = allocator,
        .connection_string = options.connection_string,
        .username = options.username,
        .password = options.password,
        .privilege = options.privilege,
    };

    return connection;
}

pub inline fn checkError(result: c_int, err: anytype) !void {
    if (result < 0) return err;
}

pub fn truncateTable(conn: *Connection, table: []const u8) !void {
    const sql = try std.fmt.allocPrint(a, "truncate table {s}", .{table});
    defer a.free(sql);
    _ = try conn.execute(sql);
}

pub fn dropTable(conn: *Connection, table: []const u8) !void {
    const sql = try std.fmt.allocPrint(a, "drop table {s}", .{table});
    defer a.free(sql);
    _ = conn.execute(sql) catch |err| {
        if (std.mem.indexOf(u8, conn.errorMessage(), "ORA-00942")) |_| {
            return error.TableNotFound;
        }
        return err;
    };
}

pub fn executeCreateTable(conn: *Connection, sql: []const u8) !void {
    _ = conn.execute(sql) catch |err| {
        if (std.mem.indexOf(u8, conn.errorMessage(), "ORA-00955")) |_| {
            return Error.NameAlreadyInUse;
        }
        return err;
    };
}

pub fn isTableExist(conn: *Connection, table: []const u8) !bool {
    const sql = try std.fmt.allocPrint(a, "select 1 from {s} where rownum = 1", .{table});
    defer a.free(sql);
    _ = conn.execute(sql) catch |err| {
        if (std.mem.indexOf(u8, conn.errorMessage(), "ORA-00942")) |_| {
            return false;
        }
        return err;
    };
    return true;
}

pub fn dropTableIfExists(conn: *Connection, table: []const u8) !void {
    if (try isTableExist(conn, table)) {
        try dropTable(conn, table);
    }
}

pub fn toDpiOracleTypeNum(type_name: []const u8) c.dpiOracleTypeNum {
    if (std.mem.eql(u8, type_name, "NUMBER")) {
        return c.DPI_ORACLE_TYPE_NUMBER;
    } else if (std.mem.eql(u8, type_name, "VARCHAR2")) {
        return c.DPI_ORACLE_TYPE_LONG_VARCHAR;
    } else if (std.mem.eql(u8, type_name, "CHAR")) {
        return c.DPI_ORACLE_TYPE_CHAR;
    } else if (std.mem.eql(u8, type_name, "DATE")) {
        return c.DPI_ORACLE_TYPE_DATE;
    } else if (std.mem.eql(u8, type_name, "TIMESTAMP")) {
        return c.DPI_ORACLE_TYPE_TIMESTAMP;
    } else {
        // todo
        unreachable;
    }
}

pub fn toDpiNativeTypeNum(type_name: []const u8) c.dpiNativeTypeNum {
    if (std.mem.eql(u8, type_name, "NUMBER")) {
        return c.DPI_NATIVE_TYPE_DOUBLE;
    } else if (std.mem.eql(u8, type_name, "VARCHAR2")) {
        return c.DPI_NATIVE_TYPE_BYTES;
    } else if (std.mem.eql(u8, type_name, "CHAR")) {
        return c.DPI_NATIVE_TYPE_BYTES;
    } else if (std.mem.eql(u8, type_name, "DATE")) {
        return c.DPI_NATIVE_TYPE_TIMESTAMP;
    } else if (std.mem.eql(u8, type_name, "TIMESTAMP")) {
        return c.DPI_NATIVE_TYPE_TIMESTAMP;
    } else {
        // todo
        unreachable;
    }
}
