const std = @import("std");
const oci = @import("c.zig").oci;
const Connection = @import("Connection.zig");
const ResultSet = @import("ResultSet.zig");

const Self = @This();

allocator: std.mem.Allocator,
oci_stmt: ?*oci.OCI_Statement = null,

conn: *Connection,

pub fn init(allocator: std.mem.Allocator, conn: *Connection) Self {
    return Self{
        .allocator = allocator,
        .conn = conn,
    };
}
pub fn deinit(self: *Self) !void {
    if (self.oci_stmt) |stmt| {
        _ = oci.OCI_StatementFree(stmt);
        self.oci_stmt = null;
    }
}

pub fn create(self: *Self) !void {
    self.oci_stmt = oci.OCI_CreateStatement(self.conn.oci_conn);
    if (self.oci_stmt == null) {
        return error.Fail;
    }
}

pub fn prepare(self: Self, sql: []const u8) !void {
    const _sql = try std.fmt.allocPrintZ(self.allocator, "{s}", .{sql});
    defer self.allocator.free(_sql);
    if (oci.OCI_Prepare(self.oci_stmt, _sql.ptr) != oci.TRUE) {
        return error.Fail;
    }
}

pub fn setFetchSize(self: Self, fetch_size: u32) !void {
    if (oci.OCI_SetFetchSize(self.oci_stmt, fetch_size) != oci.TRUE) {
        return error.Fail;
    }
}

pub fn bind(self: Self, name_or_pos: []const u8, data: anytype) !void {
    _ = self;
    _ = name_or_pos;
    _ = data;
    // * from ocilib
    // When using binding by position, provide the position to OCI_BindXXXX() call through the name parameter.
    // Within this mode the bind name must be the position preceded by a semicolon like ':1', ':2', ....
    // todo
}

pub fn execute(self: Self) !void {
    if (oci.OCI_Execute(self.oci_stmt) != oci.TRUE) {
        return error.Fail;
    }
}

pub fn getResultSet(self: *Self) !ResultSet {
    return try ResultSet.init(self.allocator, self);
}
