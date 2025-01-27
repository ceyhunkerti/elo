const std = @import("std");
const Allocator = std.mem.Allocator;
const ConnectionOptions = @import("./options.zig").ConnectionOptions;
const Connection = @import("./Connection.zig");
const w = @import("../../wire/wire.zig");
const p = @import("../../wire/proto.zig");

const c = @import("c.zig").c;

const Error = error{
    TableNotFound,
    ExpectedRecord,
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
            return error.NameAlreadyInUse;
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

// mostly will be used for test purposes
pub fn count(conn: *Connection, table_name: []const u8) !f64 {
    const sql = try std.fmt.allocPrint(a, "select count(*) from {s}", .{table_name});
    defer a.free(sql);
    var stmt = try conn.prepareStatement(sql);
    const record = try stmt.fetch(try stmt.execute());
    if (record) |r| {
        defer r.deinit(a);
        return r.get(0).Double.?;
    } else {
        return error.ExpectedRecord;
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

pub fn expectMetadata(wire: *w.Wire) !*const p.Metadata {
    const message = wire.get();
    switch (message.data) {
        .Metadata => |m| return m,
        else => {
            wire.put(message);
            return error.MetadataNotReceived;
        },
    }
}
