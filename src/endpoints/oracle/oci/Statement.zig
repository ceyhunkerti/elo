const std = @import("std");
const e = @import("error.zig");
const c = @import("c.zig").c;
const Connection = @import("Connection.zig");
const types = @import("types.zig");

pub const Error = error{
    OCIDescriptorFreeFailed,
};

const Self = @This();

pub const QueryVariableTypeInfo = struct {
    oracle_type: c_int, // c.SQLT_*
};

pub const QueryVariable = struct {
    name: []const u8,
    type_info: QueryVariableTypeInfo,
};

pub const QueryInfo = struct {
    allocator: std.mem.Allocator,
    column_count: u32 = 0,
    variables: std.ArrayList(QueryVariable),

    pub fn init(allocator: std.mem.Allocator, column_count: u32) !QueryInfo {
        return QueryInfo{
            .allocator = allocator,
            .column_count = column_count,
            .variables = try std.ArrayList(QueryVariable).initCapacity(allocator, column_count),
        };
    }

    pub fn variableName(self: QueryInfo, i: usize) []const u8 {
        return self.variables.items[i].name;
    }

    pub fn variableTypeInfo(self: QueryInfo, i: usize) QueryVariableTypeInfo {
        return self.variables.items[i].type_info;
    }

    pub fn deinit(self: *QueryInfo) void {
        self.variables.deinit();
    }
};

oci_err_handle: ?*c.OCIError = null,
oci_stmt: ?*c.OCIStmt = null,

allocator: std.mem.Allocator,
conn: *Connection = undefined,

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
    var stmt = try Self.init(std.testing.allocator, &conn);

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

    var qi = try QueryInfo.init(self.allocator, param_count);

    for (0..param_count) |i| {
        var param_handle: ?*c.OCIParam = null;
        try e.check(
            self.oci_err_handle,
            c.OCIParamGet(
                self.oci_stmt,
                c.OCI_HTYPE_STMT,
                self.oci_err_handle,
                @ptrCast(&param_handle),
                @intCast(i + 1),
            ),
        );

        var col_name: ?[*:0]u8 = null;
        var col_name_len: u32 = 0;
        try e.check(
            self.oci_err_handle,
            c.OCIAttrGet(
                param_handle,
                c.OCI_DTYPE_PARAM,
                @ptrCast(&col_name),
                @ptrCast(&col_name_len),
                c.OCI_ATTR_NAME,
                self.oci_err_handle,
            ),
        );

        var oracle_type: c_int = 0;
        try e.check(
            self.oci_err_handle,
            c.OCIAttrGet(
                param_handle,
                c.OCI_DTYPE_PARAM,
                @ptrCast(&oracle_type),
                null,
                c.OCI_ATTR_DATA_TYPE,
                self.oci_err_handle,
            ),
        );

        try qi.variables.append(.{
            .name = std.mem.sliceTo(col_name.?, 0),
            .type_info = .{
                .oracle_type = oracle_type,
            },
        });

        if (c.OCIDescriptorFree(param_handle, c.OCI_DTYPE_PARAM) < 0) {
            std.debug.print("OCIDescriptorFree failed.\n", .{});
            return error.OCIDescriptorFreeFailed;
        }
    }

    return qi;
}
test "Statement.queryInfo" {
    const allocator = std.testing.allocator;
    var conn = try Connection.init("localhost/ORCLPDB1", "demo", "demo");
    var stmt = try Self.init(allocator, &conn);
    try stmt.prepare("SELECT 1 as a1, 'hello' as B1  FROM DUAL");
    try stmt.describe();
    var qi = try stmt.queryInfo();
    defer qi.deinit();
    try std.testing.expect(qi.column_count == 2);
    try std.testing.expectEqualStrings(qi.variableName(0), "A1");
    try std.testing.expectEqual(qi.variableTypeInfo(0).oracle_type, c.SQLT_NUM);
    try std.testing.expectEqualStrings((qi.variableName(1)), "B1");
}
