const std = @import("std");
const e = @import("error.zig");
const c = @import("c.zig").c;
const Connection = @import("Connection.zig");
const QueryInfo = @import("QueryInfo.zig");

pub const Error = error{
    Error,
    OCIDescriptorFreeFailed,
};

const Self = @This();

oci_err_handle: ?*c.OCIError = null,
oci_stmt: ?*c.OCIStmt = null,
oci_define: ?*c.OCIDefine = null,

allocator: std.mem.Allocator,
conn: *Connection = undefined,
query_info: ?QueryInfo = null,

pub fn init(allocator: std.mem.Allocator, conn: *Connection) !Self {
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
        .allocator = allocator,
        .conn = conn,
        .oci_stmt = oci_stmt,
        .oci_err_handle = oci_err_handle,
    };
}

pub fn deinit(self: *Self) !void {
    if (self.query_info) |*qi| {
        qi.deinit();
    }
    // if (self.oci_stmt) |oci_stmt| {
    //     _ = c.OCIHandleFree(oci_stmt, c.OCI_HTYPE_STMT);
    // }
    // if (self.oci_err_handle) |oci_err_handle| {
    //     _ = c.OCIHandleFree(oci_err_handle, c.OCI_HTYPE_ERROR);
    // }

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
    if (self.query_info) |*qi| {
        qi.deinit();
    }
    self.query_info = try QueryInfo.init(self.allocator, self);
}

// pub fn define(self: *Self) !void {
//     if(self.query_info == null) {
//         try self.describe();
//     }
//     const qi = self.query_info.?;

//     for (0..qi.variables.items.len) |i| {
//         try e.check(
//         self.oci_err_handle,
//         c.OCIDefineByPos(
//             self.oci_stmt,
//             @ptrCast(&self.oci_define),
//             self.oci_err_handle,
//             @intCast(i + 1),
//             0,
//             null,
//             0,
//             c.OCI_DTYPE_LOB,
//         )
//         )
//     }

//   checkerr(errhp, OCIDefineByPos(stmthp, &defnp, errhp, 1, (dvoid *) &empno,
//                    (sword) sizeof(sword), SQLT_INT, (dvoid *) 0, (ub2 *)0,
//                    (ub2 *)0, OCI_DEFAULT));
// }

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
    var stmt = try Self.init(std.testing.allocator, &conn);

    try stmt.prepare("SELECT * FROM DUAL");

    try stmt.deinit();
    try conn.deinit();
}
