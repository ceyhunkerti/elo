const Record = @This();
const std = @import("std");

const p = @import("proto.zig");
const w = @import("../wire.zig");
const M = @import("../M.zig");

const Value = p.Value;
const ValueDictionary = p.ValueDictionary;

values: std.ArrayList(Value) = undefined,

pub fn init(allocator: std.mem.Allocator, size: usize) !Record {
    return .{
        .values = try std.ArrayList(Value).initCapacity(allocator, size),
    };
}

pub fn fromSlice(allocator: std.mem.Allocator, values: []const Value) !Record {
    var record = try Record.init(allocator, values.len);
    try record.appendSlice(values);
    return record;
}

pub fn Message(allocator: std.mem.Allocator, values: []const Value) !*w.Message {
    const msg = try allocator.create(w.Message);
    msg.* = .{ .data = .{ .Record = try Record.fromSlice(allocator, values) } };
    return msg;
}

pub fn deinit(self: Record, allocator: std.mem.Allocator) void {
    for (self.values.items) |it| it.deinit(allocator);
    self.values.deinit();
}

pub fn len(self: Record) usize {
    return self.values.items.len;
}

pub fn items(self: Record) []Value {
    return self.values.items;
}

pub fn capacity(self: Record) usize {
    return self.values.capacity;
}

pub fn get(self: Record, index: usize) Value {
    return self.values.items[index];
}

pub inline fn append(self: *Record, value: Value) !void {
    try self.values.append(value);
}

pub inline fn appendSlice(self: *Record, values: []const Value) !void {
    if (self.values.items.len + values.len > self.values.capacity) {
        return error.RecordFieldCapacityExceeded;
    }
    try self.values.appendSlice(values);
}

// record still owns the value memory
pub fn asMap(self: Record, allocator: std.mem.Allocator, names: []const []const u8) !ValueDictionary {
    var map = try ValueDictionary.init(allocator);
    for (names, self.values.items) |name, value| {
        try map.put(name, value);
    }
    return map;
}

pub fn asMessage(self: Record, allocator: std.mem.Allocator) !*w.Message {
    return try M.new(allocator, self);
}

pub fn write(self: Record, result: *std.ArrayList(u8), formatter: p.RecordFormatter) !void {
    const size = self.values.items.len;
    for (self.values.items, 0..) |value, i| {
        try value.write(result, formatter.value_formatter);
        if (i < size - 1) {
            try result.appendSlice(formatter.delimiters.field_delimiter);
        }
    }
}
pub fn toString(self: Record, allocator: std.mem.Allocator, formatter: p.RecordFormatter) ![]const u8 {
    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();
    try self.write(&result, formatter);
    return result.toOwnedSlice();
}

test "Record" {
    const allocator = std.testing.allocator;

    var record = try Record.init(allocator, 2);
    defer record.deinit(allocator);

    try record.append(.{ .Int = 1 });
    try record.append(.{ .String = try allocator.dupe(u8, "test") });

    try std.testing.expectEqual(@as(usize, 2), record.values.items.len);

    var map = try record.asMap(allocator, &[_][]const u8{ "a", "b" });
    defer map.deinit();
    try std.testing.expectEqual(@as(usize, 2), map.count());
}
