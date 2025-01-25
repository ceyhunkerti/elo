const std = @import("std");
const w = @import("wire.zig");
const M = @import("M.zig");

const StringHashMap = std.StringHashMap;

const Error = error{
    RecordFieldCapacityExceeded,
};

pub const Timestamp = struct {
    year: i16,
    month: u8,
    day: u8,
    hour: u8 = 0,
    minute: u8 = 0,
    second: u8 = 0,
    nanosecond: u32 = 0,
    tz_offset: struct {
        hours: i8 = 0,
        minutes: i8 = 0,
    } = .{},
};

pub const FieldType = enum {
    String,
    Int,
    Double,
    TimeStamp,
    Number,
    Boolean,
    Array,
    Json,
};

pub const Metadata = struct {
    name: []const u8,
    fields: []const Field,

    pub fn deinit(self: Metadata, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.fields) |field| field.deinit(allocator);
        allocator.free(self.fields);
    }
};

pub const Field = struct {
    index: usize,
    name: ?[]const u8 = null,
    type: ?FieldType = null,
    default: ?Value = null,
    description: ?[]const u8 = null,
    nullable: bool = true,
    length: ?u32 = null,
    precision: ?u8 = null,
    scale: ?u8 = null,

    pub fn deinit(self: Field, allocator: std.mem.Allocator) void {
        if (self.name) |name| allocator.free(name);
        if (self.description) |description| allocator.free(description);
    }
};

pub const Value = union(FieldType) {
    String: ?[]u8,
    Int: ?i64,
    Double: ?f64,
    TimeStamp: ?Timestamp,
    Number: ?f64,
    Boolean: ?bool,
    Array: ?[]Value,
    Json: ?ValueDictionary,

    pub fn deinit(self: Value, allocator: std.mem.Allocator) void {
        switch (self) {
            .String => |str| if (str) |s| allocator.free(s),
            // todo
            // .Map => |map| if (map) |m| m.deinit(),
            // .Json => |json| if (json) |j| j.deinit(allocator),
            .Array => |arr| if (arr) |a| {
                for (a) |item| item.deinit(allocator);
                allocator.free(a);
            },
            // todo
            else => {},
        }
    }
};

pub const ValueDictionary = struct {
    dict: std.StringHashMap(Value),

    pub fn init(allocator: std.mem.Allocator) !ValueDictionary {
        return .{
            .dict = std.StringHashMap(Value).init(allocator),
        };
    }
    pub fn deinit(self: *ValueDictionary) void {
        self.dict.deinit();
    }

    pub fn put(self: *ValueDictionary, name: []const u8, value: Value) !void {
        try self.dict.put(name, value);
    }
    pub fn get(self: ValueDictionary, name: []const u8) ?Value {
        return self.dict.get(name);
    }
    pub fn count(self: ValueDictionary) usize {
        return self.dict.count();
    }
};

pub const Record = struct {
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

    pub fn item(self: Record, index: usize) Value {
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
};

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
