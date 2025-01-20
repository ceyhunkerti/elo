// cdemodp.c

const std = @import("std");
const c = @import("c.zig").c;
const e = @import("error.zig");

const Error = error{
    FailedToInitializeEnv,
    ServerAttachFailed,
    ErrorHandleInitFailed,
    ConnectionError,
};

const Self = @This();

// oci variables
oci_err_handle: ?*c.OCIError = null, // oci environment handle
oci_env: ?*c.OCIEnv = null, // server handle
oci_server: ?*c.OCIServer = null, // service context
oci_service_context: ?*c.OCISvcCtx = null,
oci_session: ?*c.OCISession = null,

//
connection_string: []const u8,
username: []const u8,
password: []const u8,

pub fn init(
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
) !Self {
    var error_code: i32 = 0;
    var oci_env: ?*c.OCIEnv = null;
    var oci_server: ?*c.OCIServer = null;
    var oci_service_context: ?*c.OCISvcCtx = null;
    var oci_err_handle: ?*c.OCIError = null;
    var oci_session: ?*c.OCISession = null;

    if (c.OCIInitialize(@intCast(c.OCI_DEFAULT), null, null, null, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIInitialize failed\n", .{});
        return error.FailedToInitialize;
    }
    if (c.OCIEnvInit(&oci_env, c.OCI_DEFAULT, 0, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIEnvInit failed\n", .{});
        return error.FailedToInitialize;
    }
    if (c.OCIHandleAlloc(oci_env, @ptrCast(&oci_server), c.OCI_HTYPE_SERVER, 0, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIHandleAlloc failed\n", .{});
        return error.FailedToInitialize;
    }
    if (c.OCIHandleAlloc(oci_env, @ptrCast(&oci_service_context), c.OCI_HTYPE_SVCCTX, 0, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIHandleAlloc failed\n", .{});
        return error.FailedToInitialize;
    }
    if (c.OCIHandleAlloc(oci_env, @ptrCast(&oci_err_handle), c.OCI_HTYPE_ERROR, 0, null) != c.OCI_SUCCESS) {
        std.debug.print("OCIHandleAlloc failed.\n", .{});
        return error.ErrorHandleInitFailed;
    }

    if (c.OCIServerAttach(
        oci_server,
        oci_err_handle,
        connection_string.ptr,
        @intCast(connection_string.len),
        c.OCI_DEFAULT,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCIServerAttach failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ServerAttachFailed;
    }
    if (c.OCIAttrSet(
        oci_service_context,
        c.OCI_HTYPE_SVCCTX,
        oci_server,
        0,
        c.OCI_ATTR_SERVER,
        oci_err_handle,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCIAttrSet failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ConnectionError;
    }

    if (c.OCIHandleAlloc(
        oci_env,
        @ptrCast(&oci_session),
        c.OCI_HTYPE_SESSION,
        0,
        null,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCIHandleAlloc failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ConnectionError;
    }

    if (c.OCIAttrSet(
        oci_session,
        c.OCI_HTYPE_SESSION,
        @constCast(@ptrCast(username.ptr)),
        @intCast(username.len),
        c.OCI_ATTR_USERNAME,
        oci_err_handle,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCIAttrSet username failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ConnectionError;
    }

    if (c.OCIAttrSet(
        oci_session,
        c.OCI_HTYPE_SESSION,
        @constCast(@ptrCast(password.ptr)),
        @intCast(password.len),
        c.OCI_ATTR_PASSWORD,
        oci_err_handle,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCIAttrSet password failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ConnectionError;
    }

    if (c.OCISessionBegin(
        oci_service_context,
        oci_err_handle,
        oci_session,
        c.OCI_CRED_RDBMS,
        c.OCI_DEFAULT,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCISessionBegin failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ConnectionError;
    }

    if (c.OCIAttrSet(
        oci_service_context,
        c.OCI_HTYPE_SVCCTX,
        oci_session,
        0,
        c.OCI_ATTR_SESSION,
        oci_err_handle,
    ) != c.OCI_SUCCESS) {
        std.debug.print("OCIAttrSet connection_string failed.\n", .{});
        try e.printError(oci_err_handle, c.OCI_HTYPE_ERROR, &error_code);
        return error.ConnectionError;
    }

    return .{
        .oci_env = oci_env,
        .oci_server = oci_server,
        .oci_service_context = oci_service_context,
        .oci_err_handle = oci_err_handle,

        .connection_string = connection_string,
        .username = username,
        .password = password,
    };
}

test "Connection.serverAttach" {
    _ = try Self.init("localhost/ORCLPDB1", "demo", "demo");
}
