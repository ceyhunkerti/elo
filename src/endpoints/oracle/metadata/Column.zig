const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @import("../c.zig").c;

const oci = @import("../c.zig").oci;
const p = @import("../../../wire/proto.zig");

const Statement = @import("../Statement.zig");

const Self = @This();

allocator: Allocator = undefined,
oci_column: ?*oci.OCI_Column = null,

index: u32,
name: []const u8,
nullable: bool = true,
type: u32 = 0,
sub_type: u32 = 0,
sql_type: []const u8 = undefined,
size: u32 = 0,
precision: i32 = 0,
scale: i32 = 0,

pub fn init(allocator: Allocator, index: u32, oci_column: *oci.OCI_Column) Self {
    return .{
        .allocator = allocator,
        .oci_column = oci_column,
        .index = index,
        .name = allocator.dupe(u8, std.mem.sliceTo(oci.OCI_ColumnGetName(oci_column), 0)) catch unreachable,
        .nullable = oci.OCI_ColumnGetNullable(oci_column) == oci.TRUE,
        .type = oci.OCI_ColumnGetType(oci_column),
        .sub_type = oci.OCI_ColumnGetSubType(oci_column),
        .sql_type = allocator.dupe(u8, std.mem.sliceTo(oci.OCI_ColumnGetSQLType(oci_column), 0)) catch unreachable,
        .size = oci.OCI_ColumnGetSize(oci_column),
        .precision = oci.OCI_ColumnGetPrecision(oci_column),
        .scale = oci.OCI_ColumnGetScale(oci_column),
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.name);
    self.allocator.free(self.sql_type);
}

pub fn toString(self: Self) ![]const u8 {
    var buffer: [512]u8 = std.mem.zeroes([512]u8);
    defer self.allocator.free(&buffer);

    if (oci.OCI_ColumnGetFullSQLType(self.oci_column, @ptrCast(&buffer), 512) != oci.TRUE) {
        return error.Fail;
    }
    return try std.fmt.allocPrint(self.allocator, "{s} {s}", .{ self.name, buffer });
}
