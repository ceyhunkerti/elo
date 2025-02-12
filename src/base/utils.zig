const std = @import("std");

fn parseFieldValue(comptime T: type, value: ?[]const u8, default_value: ?T) !T {
    if (value == null and default_value != null) {
        return default_value.?;
    }

    return switch (@typeInfo(T)) {
        .Optional => |opt_info| if (value) |v|
            try parseFieldValue(opt_info.child, v)
        else
            null,
        .Pointer => |ptr_info| switch (ptr_info.size) {
            .Slice => value orelse return error.MissingField,
            else => @compileError("Unsupported pointer type"),
        },
        .Int => |int_info| switch (int_info.signedness) {
            .signed => try std.fmt.parseInt(T, value orelse return error.MissingField, 10),
            .unsigned => try std.fmt.parseInt(T, value orelse return error.MissingField, 10),
        },
        .Float => try std.fmt.parseFloat(T, value orelse return error.MissingField),
        .Bool => if (std.mem.eql(u8, value orelse return error.MissingField, "true")) true else false,
        else => @compileError("Unsupported field type: " ++ @typeName(T)),
    };
}

// This function takes a generic type T and a StringHashMap([]const u8) and returns a value of type T
pub fn fromMap(comptime T: type, map: std.StringHashMap([]const u8)) !T {
    var result: T = undefined;

    const fields = std.meta.fields(T);

    inline for (fields) |field| {
        const value = map.get(field.name) orelse return error.MissingField;
        const default_value = if (field.default_value) |ptr|
            @as(*const field.type, @ptrCast(@alignCast(ptr))).*
        else
            null;
        @field(result, field.name) = try parseFieldValue(field.type, value, default_value);
    }

    return result;
}

fn parseValueOwned(allocator: std.mem.Allocator, comptime T: type, value: ?[]const u8, default_value: ?T) !T {
    if (value == null and default_value != null) {
        return default_value.?;
    }

    return switch (@typeInfo(T)) {
        .Optional => |opt_info| if (value) |v|
            try parseValueOwned(allocator, opt_info.child, v)
        else
            null,
        .Pointer => |ptr_info| switch (ptr_info.size) {
            .Slice => if (value) |v| try allocator.dupe(u8, v) else return error.MissingField,
            else => @compileError("Unsupported pointer type"),
        },
        .Int => |int_info| switch (int_info.signedness) {
            .signed => try std.fmt.parseInt(T, value orelse return error.MissingField, 10),
            .unsigned => try std.fmt.parseInt(T, value orelse return error.MissingField, 10),
        },
        .Float => try std.fmt.parseFloat(T, value orelse return error.MissingField),
        .Bool => if (std.mem.eql(u8, value orelse return error.MissingField, "true")) true else false,
        else => @compileError("Unsupported field type: " ++ @typeName(T)),
    };
}

pub fn fromMapOwned(allocator: std.mem.Allocator, comptime T: type, map: std.StringHashMap([]const u8)) !T {
    var result: T = undefined;

    const fields = std.meta.fields(T);

    inline for (fields) |field| {
        const value = map.get(field.name) orelse return error.MissingField;
        @field(result, field.name) = try parseValueOwned(allocator, field.type, value);
    }

    return result;
}

// Example usage:
const Person = struct {
    name: []const u8,
    age: u32,
    active: bool,
};

test "fromMap basic usage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var map = std.StringHashMap([]const u8).init(allocator);
    try map.put("name", "John");
    try map.put("age", "30");
    try map.put("active", "true");

    const person = try fromMap(Person, map);

    try std.testing.expectEqualStrings("John", person.name);
    try std.testing.expectEqual(@as(u32, 30), person.age);
    try std.testing.expect(person.active);
}
