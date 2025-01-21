const std = @import("std");
const c = @import("../oci/c.zig").c;
const Connection = @import("../oci/Connection.zig");
const e = @import("../oci/error.zig");

pub const Error = error{
    ErrorHandleInitFailed,
    DirectPathCtxInitFailed,
};

const Self = @This();

oci_err_handle: ?*c.OCIError = null, // oci error handle
oci_direct_path_ctx: ?*c.OCIDbContext = null,

conn: *Connection = undefined,

pub fn init(conn: *Connection) Self {
    var oci_err_handle: ?*c.OCIError = null;

    // init error handle
    if (c.OCIHandleAlloc(conn.oci_env, @ptrCast(&oci_err_handle), c.OCI_HTYPE_ERROR, 0, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIHandleAlloc failed.\n", .{});
        return error.ErrorHandleInitFailed;
    }

    return .{
        .conn = conn,
        .oci_err_handle = oci_err_handle,
    };
}

pub fn prepareLoader(self: *Self) !void {
    var error_code: i32 = 0;

    if (c.OCIHandleAlloc(self.conn.oci_env, @ptrCast(&self.oci_direct_path_ctx), c.OCI_HTYPE_DIRPATH_CTX, 0, null) != c.OCI_SUCCESS) {
        e.printError(self.oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.DirectPathCtxInitFailed;
    }
}
