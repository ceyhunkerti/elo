const std = @import("std");
const b = @import("base");

pub fn TypeInfo(comptime T: type) type {
    return struct {
        field_type: b.FieldType = undefined,
        size: ?u32 = null,
        precision: ?u32 = null,
        scale: ?u32 = null,
        nullable: ?bool = null,
        default: ?b.Value = null,

        ext: ?T = null,

        pub fn toString(self: TypeInfo, allocator: std.mem.Allocator) ![]const u8 {
            var result = std.ArrayList(u8).init(allocator);
            defer result.deinit();

            try result.append(@tagName(self.field_type));
            if (self.size) |sz| try result.writer().print("({d})", .{sz});
            return result.toOwnedSlice();
        }
    };
}

pub fn Column(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator = undefined,
        index: u32,
        name: []const u8 = undefined,
        type_info: ?TypeInfo(T) = null,

        pub fn init(allocator: std.mem.Allocator, index: u32, name: []const u8, type_info: ?TypeInfo(T)) Column(T) {
            return .{
                .allocator = allocator,
                .index = index,
                .name = name,
                .type_info = type_info,
            };
        }

        pub fn deinit(self: Self) void {
            self.allocator.free(self.name);

            if (self.type_info) |ti| {
                if (ti.default) |d| d.deinit(self.allocator);
                comptime {
                    if (@hasField(T, "deinit")) {
                        if (self.type_info.?.ext) |ext| ext.deinit();
                    }
                }
            }
        }

        pub fn toString(self: Self) ![]const u8 {
            if (self.type_info) |ti| {
                const ts = try ti.toString(self.allocator);
                defer self.allocator.free(ts);
                return std.fmt.allocPrint(self.allocator, "{s} {s}", .{ self.name, ts });
            }
            return try self.allocator.dupe(u8, self.name);
        }
    };
}
