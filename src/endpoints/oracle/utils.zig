const std = @import("std");
const Allocator = std.mem.Allocator;
const ConnectionOptions = @import("./options.zig").ConnectionOptions;
const Connection = @import("./Connection.zig");
const w = @import("../../wire/wire.zig");
const p = @import("../../wire/proto.zig");

const oci = @import("c.zig").oci;

const Error = error{
    TableNotFound,
    ExpectedRecord,
    NoDataFound,
};

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const a = gpa.allocator();

pub fn initConnection(allocator: Allocator, options: ConnectionOptions) Connection {
    return Connection.init(
        allocator,
        options.connection_string,
        options.username,
        options.password,
        options.privilege,
    ) catch unreachable;
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
        if (std.mem.indexOf(u8, conn.getLastError(), "ORA-00942")) |_| {
            return error.TableNotFound;
        }
        return err;
    };
}

pub fn executeCreateTable(conn: *Connection, sql: []const u8) !void {
    _ = conn.execute(sql) catch |err| {
        if (std.mem.indexOf(u8, conn.getLastError(), "ORA-00955")) |_| {
            return error.NameAlreadyInUse;
        }
        return err;
    };
}

pub fn isTableExist(conn: *Connection, table: []const u8) !bool {
    const sql = try std.fmt.allocPrint(a, "select 1 as a from {s} where rownum = 1", .{table});

    defer a.free(sql);
    var stmt = try conn.prepareStatement(sql);
    defer stmt.deinit() catch unreachable;
    stmt.execute() catch |err| {
        const errm = conn.getLastError();
        defer conn.allocator.free(errm);
        if (std.mem.indexOf(u8, errm, "ORA-00942")) |_| {
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
    defer stmt.deinit();
    try stmt.execute();
    var rs = try stmt.getResultSet();
    defer rs.deinit();

    if (try rs.next()) |r| {
        defer r.deinit(a);
        return r.get(0).Double.?;
    } else {
        return error.NoDataFound;
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
