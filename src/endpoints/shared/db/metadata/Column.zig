const Column = @This();

const std = @import("std");
const p = @import("../../../../wire/proto/proto.zig");

pub const TypeInfo = struct {
    field_type: p.FieldType = undefined,
    size: ?u32 = null,
    precision: ?u32 = null,
    scale: ?u32 = null,
    default: ?p.Value = null,
    nullable: ?bool = null,

    // database specific name
    vendor_type_name: ?[]const u8 = null,
    // database specific id
    vendor_type_id: ?u32 = null,
    // database specific native type id
    native_type_id: ?u32 = null,

    pub fn toString(self: TypeInfo) ![]const u8 {
        var result = std.ArrayList(u8).init(self.allocator);
        defer result.deinit();

        try result.append(@tagName(self.field_type));
        if (self.size) |sz| try result.writer().print("({d})", .{sz});
        return result.toOwnedSlice();
    }
};

allocator: std.mem.Allocator = undefined,
index: u32,
name: []const u8 = undefined,
type_info: ?TypeInfo = null,

pub fn init(allocator: std.mem.Allocator, index: u32, name: []const u8, type_info: ?TypeInfo) Column {
    return .{
        .allocator = allocator,
        .index = index,
        .name = name,
        .type_info = type_info,
    };
}

pub fn deinit(self: *Column) void {
    self.allocator.free(self.name);
    if (self.type_info) |ti| {
        if (ti.default) |d| self.allocator.free(d);
        if (ti.vendor_type_name) |vtn| self.allocator.free(vtn);
    }
}

pub fn toString(self: Column) ![]const u8 {
    if (self.type_info) |ti| {
        const ts = try ti.toString();
        defer self.allocator.free(ts);
        return std.fmt.allocPrint(self.allocator, "{s} {s}", .{ self.name, ts });
    }
    return try self.allocator.dupe(u8, self.name);
}
