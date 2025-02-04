const std = @import("std");
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

pub fn isTableExists(allocator: std.mem.Allocator, conn: *Connection, table_name: []const u8) bool {
    const sql = try std.fmt.allocPrintZ(
        allocator,
        \\select count(1) a c from information_schema.tables where upper(table_name) = upper('{s}')
        \\and upper(table_schema) = upper('{s}')
        \\and table_catalog = ('{s}')
    ,
        .{ table_name, conn.username, conn.database },
    );
    defer allocator.free(sql);
    var cursor = try conn.createCursor("test_cursor", sql);
    defer {
        cursor.close() catch unreachable;
        cursor.deinit();
    }
    const row_count = try cursor.execute();
    std.debug.assert(row_count == 1);
    const record = try cursor.fetchNext();
    std.debug.assert(record != null);
    return record.?.get(1).Int != 0;
}
