const std = @import("std");
const e = @import("error.zig");
const c = @import("c.zig").c;
const Connection = @import("Connection.zig");

const Self = @This();

pub const QueryInfo = struct {
    column_count: u32 = 0,
};

oci_err_handle: ?*c.OCIError = null,
oci_stmt: ?*c.OCIStmt = null,

conn: *Connection = undefined,

pub fn init(conn: *Connection) !Self {
    var oci_stmt: ?*c.OCIStmt = null;

    var oci_err_handle: ?*c.OCIError = null;

    try e.check(
        conn.oci_err_handle,
        c.OCIHandleAlloc(conn.oci_env, @ptrCast(&oci_stmt), c.OCI_HTYPE_STMT, 0, null),
    );

    // init error handle
    if (c.OCIHandleAlloc(conn.oci_env, @ptrCast(&oci_err_handle), c.OCI_HTYPE_ERROR, 0, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIHandleAlloc failed.\n", .{});
        return error.ErrorHandleInitFailed;
    }

    return .{
        .conn = conn,
        .oci_stmt = oci_stmt,
        .oci_err_handle = oci_err_handle,
    };
}

pub fn prepare(self: *Self, sql: []const u8) !void {
    try e.check(
        self.oci_err_handle,
        c.OCIStmtPrepare(
            self.oci_stmt,
            self.oci_err_handle,
            @ptrCast(sql.ptr),
            @intCast(sql.len),
            c.OCI_NTV_SYNTAX,
            c.OCI_DEFAULT,
        ),
    );
}

pub fn describe(self: *Self) !void {
    try e.check(
        self.oci_err_handle,
        c.OCIStmtExecute(
            self.conn.oci_service_context,
            self.oci_stmt,
            self.oci_err_handle,
            0,
            0,
            null,
            null,
            c.OCI_DESCRIBE_ONLY,
        ),
    );
}

pub fn execute(self: *Self) !void {
    try e.check(
        self.oci_err_handle,
        c.OCIStmtExecute(
            self.conn.oci_service_context,
            self.oci_stmt,
            self.oci_err_handle,
            1,
            0,
            null,
            null,
            c.OCI_DEFAULT,
        ),
    );
}

test "Statement.[init,prepare]" {
    var conn = try Connection.init("localhost/ORCLPDB1", "demo", "demo");
    var stmt = try Self.init(&conn);

    try stmt.prepare("SELECT * FROM DUAL");

    try conn.deinit();
}

pub fn queryInfo(self: *Self) !QueryInfo {
    var param_count: u32 = 0;

    try e.check(
        self.conn.oci_err_handle,
        c.OCIAttrGet(
            self.oci_stmt,
            c.OCI_HTYPE_STMT,
            &param_count,
            null,
            c.OCI_ATTR_PARAM_COUNT,
            self.conn.oci_err_handle,
        ),
    );
    return .{
        .column_count = param_count,
    };
}
test "Statement.queryInfo" {
    var conn = try Connection.init("localhost/ORCLPDB1", "demo", "demo");
    var stmt = try Self.init(&conn);
    try stmt.prepare("SELECT 1 as a, 'hello' as b  FROM DUAL");
    try stmt.describe();
    const qi = try stmt.queryInfo();
    try std.testing.expect(qi.column_count == 2);
}
