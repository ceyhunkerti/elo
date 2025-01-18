const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @import("../c.zig").c;
const Statement = @import("../Statement.zig");
const Self = @This();
const FieldValue = @import("../../../commons.zig").FieldValue;

allocator: Allocator = undefined,
index: u32,
name: []const u8 = undefined,
nullable: bool = true,
oracle_type_num: c.dpiOracleTypeNum = undefined,
native_type_num: c.dpiNativeTypeNum = undefined,
oracle_type_name: ?[]const u8 = null,
length: u32 = 0,
precision: ?u32 = null,
scale: ?u32 = null,
default: ?[]const u8 = null,

pub fn deinit(self: *Self) void {
    self.allocator.free(self.name);
    if (self.default) |d| self.allocator.free(d);
    if (self.oracle_type_name) |otn| self.allocator.free(otn);
}

pub fn toString(self: Self) ![]const u8 {
    return try std.fmt.allocPrint(self.allocator,
        \\Name: {s}
        \\OracleTypeNme: {s}
        \\OracleTypeNum: {d}
        \\NativeTypeNum: {d}
        \\Nullable: {any}
    , .{
        self.name,
        if (self.oracle_type_name) |otn| otn else "null",
        self.oracle_type_num,
        self.native_type_num,
        self.nullable,
    });
}

pub fn fromStatement(allocator: Allocator, index: u32, stmt: *Statement) !Self {
    var info: c.dpiQueryInfo = undefined;
    if (c.dpiStmt_getQueryInfo(stmt.stmt, index, &info) < 0) {
        return error.FailedToDpiGetQueryInfo;
    }
    return .{
        .allocator = allocator,
        .index = index,
        .name = allocator.dupe(u8, std.mem.span(info.name)) catch unreachable,
        .nullable = info.nullOk > 0,
        .oracle_type_num = info.typeInfo.oracleTypeNum,
        .native_type_num = info.typeInfo.defaultNativeTypeNum,
    };
}

pub fn dpiVarSize(self: Self) u32 {
    return switch (self.native_type_num) {
        c.DPI_NATIVE_TYPE_BYTES => self.length,
        c.DPI_NATIVE_TYPE_INT64 => 0,
        else => 0,
    };
}
