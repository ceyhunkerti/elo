const std = @import("std");
const Statement = @import("Statement.zig");
const Connection = @import("Connection.zig");

const c = @import("c.zig").c;
const e = @import("error.zig");

pub const Error = error{ Error, OCIDescriptorFreeFailed };

const Self = @This();

pub const TypeInfo = struct {
    sql_type: c_int, // c.SQLT_*

};

pub const Variable = struct {
    name: []const u8,
    name_len: u32 = 0,
    type_info: TypeInfo,
};

allocator: std.mem.Allocator,
stmt: *Statement,

column_count: u32 = 0,
variables: []Variable = undefined,

pub fn init(allocator: std.mem.Allocator, stmt: *Statement) !Self {
    var param_count: u32 = 0;

    try e.check(
        stmt.oci_err_handle,
        c.OCIAttrGet(
            stmt.oci_stmt,
            c.OCI_HTYPE_STMT,
            &param_count,
            null,
            c.OCI_ATTR_PARAM_COUNT,
            stmt.oci_err_handle,
        ),
    );

    const variables = try allocator.alloc(Variable, param_count);
    for (variables, 0..) |*v, i| {
        var param_handle: ?*c.OCIParam = null;
        try e.check(
            stmt.oci_err_handle,
            c.OCIParamGet(
                stmt.oci_stmt,
                c.OCI_HTYPE_STMT,
                stmt.oci_err_handle,
                @ptrCast(&param_handle),
                @intCast(i + 1),
            ),
        );

        var col_name: ?[*:0]u8 = null;
        var col_name_len: u32 = 0;
        try e.check(
            stmt.oci_err_handle,
            c.OCIAttrGet(
                param_handle,
                c.OCI_DTYPE_PARAM,
                @ptrCast(&col_name),
                @ptrCast(&col_name_len),
                c.OCI_ATTR_NAME,
                stmt.oci_err_handle,
            ),
        );

        v.name = std.mem.sliceTo(col_name.?, 0);
        v.name_len = col_name_len;
        try setTypeInfo(stmt, param_handle, &v.type_info);

        if (c.OCIDescriptorFree(param_handle, c.OCI_DTYPE_PARAM) < 0) {
            std.debug.print("OCIDescriptorFree failed.\n", .{});
            return error.OCIDescriptorFreeFailed;
        }
    }

    return .{
        .allocator = allocator,
        .stmt = stmt,
        .column_count = param_count,
        .variables = variables,
    };
}

fn setTypeInfo(stmt: *Statement, param_handle: ?*c.OCIParam, type_info: *TypeInfo) !void {
    var sql_type: c_int = 0;
    try e.check(
        stmt.oci_err_handle,
        c.OCIAttrGet(
            param_handle,
            c.OCI_DTYPE_PARAM,
            @ptrCast(&sql_type),
            null,
            c.OCI_ATTR_DATA_TYPE,
            stmt.oci_err_handle,
        ),
    );
    type_info.sql_type = sql_type;
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.variables);
}
test "QueryInfo" {
    var conn = try Connection.init("localhost/ORCLPDB1", "demo", "demo");
    var stmt = try Statement.init(std.testing.allocator, &conn);
    try stmt.prepare("SELECT 1 as a1, 'hello' as B1  FROM DUAL");
    try stmt.describe();

    const qi = stmt.query_info.?;

    try std.testing.expect(qi.column_count == 2);
    try std.testing.expectEqualStrings(qi.variables[0].name, "A1");
    try std.testing.expectEqual(qi.variables[0].type_info.sql_type, c.SQLT_NUM);
    try std.testing.expectEqualStrings((qi.variables[1].name), "B1");
    try std.testing.expectEqual(qi.variables[1].type_info.sql_type, c.SQLT_AFC);

    try stmt.deinit();
    try conn.deinit();
}
