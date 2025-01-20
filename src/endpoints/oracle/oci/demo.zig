const std = @import("std");
const c = @cImport({
    @cInclude("oci.h");
    @cInclude("stdio.h");
    @cInclude("stdlib.h");
});

// Global OCI handles
var env: ?*c.OCIEnv = null;
var err: ?*c.OCIError = null;
var svc: ?*c.OCISvcCtx = null;
var srv: ?*c.OCIServer = null;
var sess: ?*c.OCISession = null;
var dpctx: ?*c.OCIDirPathCtx = null;
var colArray: ?*c.OCIDirPathColArray = null;
var stream: ?*c.OCIDirPathStream = null;

// Error handling function
fn checkError(error_handle: ?*c.OCIError, status: c_int) !void {
    if (status != c.OCI_SUCCESS) {
        var errbuf: [512]u8 = undefined;
        var errcode: c_int = 0;
        _ = c.OCIErrorGet(
            error_handle,
            1,
            null,
            &errcode,
            &errbuf[0],
            @sizeOf(@TypeOf(errbuf)),
            c.OCI_HTYPE_ERROR,
        );
        std.debug.print("Error - {any}\n", .{errbuf});
        return error.OCIError;
    }
}

fn initOCI() !void {
    try checkError(null, c.OCIEnvCreate(
        &env,
        c.OCI_DEFAULT,
        null,
        null,
        null,
        null,
        0,
        null,
    ));

    try checkError(null, c.OCIHandleAlloc(
        env,
        @ptrCast(&err),
        c.OCI_HTYPE_ERROR,
        0,
        null,
    ));

    try checkError(err, c.OCIHandleAlloc(
        env,
        @ptrCast(&srv),
        c.OCI_HTYPE_SERVER,
        0,
        null,
    ));

    const connect_str = "//localhost/ORCLPDB1";
    try checkError(err, c.OCIServerAttach(
        srv,
        err,
        @ptrCast(connect_str.ptr),
        @intCast(connect_str.len),
        c.OCI_DEFAULT,
    ));

    try checkError(err, c.OCIHandleAlloc(
        env,
        @ptrCast(&svc),
        c.OCI_HTYPE_SVCCTX,
        0,
        null,
    ));

    try checkError(err, c.OCIAttrSet(
        svc,
        c.OCI_HTYPE_SVCCTX,
        srv,
        0,
        c.OCI_ATTR_SERVER,
        err,
    ));

    try checkError(err, c.OCIHandleAlloc(
        env,
        @ptrCast(&sess),
        c.OCI_HTYPE_SESSION,
        0,
        null,
    ));

    const username = "demo";
    const password = "demo";

    try checkError(err, c.OCIAttrSet(
        sess,
        c.OCI_HTYPE_SESSION,
        @constCast(@ptrCast(username.ptr)),
        @intCast(username.len),
        c.OCI_ATTR_USERNAME,
        err,
    ));

    try checkError(err, c.OCIAttrSet(
        sess,
        c.OCI_HTYPE_SESSION,
        @constCast(@ptrCast(password.ptr)),
        @intCast(password.len),
        c.OCI_ATTR_PASSWORD,
        err,
    ));

    try checkError(err, c.OCISessionBegin(
        svc,
        err,
        sess,
        c.OCI_CRED_RDBMS,
        c.OCI_DEFAULT,
    ));

    try checkError(err, c.OCIAttrSet(
        svc,
        c.OCI_HTYPE_SVCCTX,
        sess,
        0,
        c.OCI_ATTR_SESSION,
        err,
    ));
}

fn performDirectPathLoad() !void {
    try checkError(err, c.OCIHandleAlloc(
        env,
        @ptrCast(&dpctx),
        c.OCI_HTYPE_DIRPATH_CTX,
        0,
        null,
    ));

    try checkError(err, c.OCIHandleAlloc(
        env,
        @ptrCast(&colArray),
        c.OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
        0,
        null,
    ));

    try checkError(err, c.OCIHandleAlloc(
        env,
        @ptrCast(&stream),
        c.OCI_HTYPE_DIRPATH_STREAM,
        0,
        null,
    ));

    const table_name = "TEST_TABLE";
    try checkError(err, c.OCIAttrSet(
        dpctx,
        c.OCI_HTYPE_DIRPATH_CTX,
        @constCast(@ptrCast(table_name.ptr)),
        @intCast(table_name.len),
        c.OCI_ATTR_NAME,
        err,
    ));

    try checkError(err, c.OCIDirPathPrepare(dpctx, svc, err));

    const id_value = "1";
    const name_value = "John Doe";

    try checkError(err, c.OCIDirPathColArrayEntrySet(
        colArray,
        err,
        0, // rownum
        0, // colIdx
        @constCast(@ptrCast(id_value.ptr)),
        @intCast(id_value.len),
        c.OCI_DIRPATH_COL_COMPLETE,
    ));

    try checkError(err, c.OCIDirPathColArrayEntrySet(
        colArray,
        err,
        0, // rownum
        1, // colIdx
        @constCast(@ptrCast(name_value.ptr)),
        @intCast(name_value.len),
        c.OCI_DIRPATH_COL_COMPLETE,
    ));

    // try checkError(err, c.OCIDirPathColArrayToStream(colArray, stream, err));

    const rowCount: c.ub4 = 1; // Example row count
    const rowOffset: c.ub4 = 0; // Example row offset

    try checkError(err, c.OCIDirPathColArrayToStream(colArray, dpctx, stream, err, rowCount, rowOffset));

    try checkError(err, c.OCIDirPathLoadStream(dpctx, stream, err));
    try checkError(err, c.OCIDirPathFinish(dpctx, err));
}

fn cleanupOCI() void {
    if (sess) |session| _ = c.OCISessionEnd(svc, err, session, c.OCI_DEFAULT);
    if (srv) |server| _ = c.OCIServerDetach(server, err, c.OCI_DEFAULT);
    if (dpctx) |context| _ = c.OCIHandleFree(context, c.OCI_HTYPE_DIRPATH_CTX);
    if (colArray) |array| _ = c.OCIHandleFree(array, c.OCI_HTYPE_DIRPATH_COLUMN_ARRAY);
    if (stream) |stream_handle| _ = c.OCIHandleFree(stream_handle, c.OCI_HTYPE_DIRPATH_STREAM);
    if (svc) |svc_ctx| _ = c.OCIHandleFree(svc_ctx, c.OCI_HTYPE_SVCCTX);
    if (srv) |srv_handle| _ = c.OCIHandleFree(srv_handle, c.OCI_HTYPE_SERVER);
    if (err) |err_handle| _ = c.OCIHandleFree(err_handle, c.OCI_HTYPE_ERROR);
    if (env) |env_handle| _ = c.OCIHandleFree(env_handle, c.OCI_HTYPE_ENV);
}

pub fn main() !void {
    try initOCI();
    try performDirectPathLoad();
    cleanupOCI();
}

test "main" {
    try main();
}
