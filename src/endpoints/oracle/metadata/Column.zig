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
length: u32 = 0,
precision: ?u32 = null,
scale: ?u32 = null,
default: ?[]const u8 = null,

pub fn deinit(self: *Self) void {
    self.allocator.free(self.name);
    if (self.default) |d| self.allocator.free(d);
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

pub fn fromFieldValue(index: u32, value: FieldValue) !Self {
    var oracle_type_num: c.dpiOracleTypeNum = undefined;
    var native_type_num: c.dpiNativeTypeNum = undefined;
    switch (value) {
        .Int => {
            oracle_type_num = c.DPI_ORACLE_TYPE_NUMBER;
            native_type_num = c.DPI_NATIVE_TYPE_INT64;
        },
        .String => {
            oracle_type_num = c.DPI_ORACLE_TYPE_VARCHAR;
            native_type_num = c.DPI_NATIVE_TYPE_VARCHAR;
        },
        else => unreachable, // TODO
    }
    return .{
        .index = index,
        .oracle_type_num = oracle_type_num,
        .native_type_num = native_type_num,
    };
}

pub fn dpiVarSize(self: Self) u32 {
    return switch (self.native_type_num) {
        c.DPI_NATIVE_TYPE_BYTES => self.length,
        c.DPI_NATIVE_TYPE_INT64 => 0,
        else => 0,
    };
}
