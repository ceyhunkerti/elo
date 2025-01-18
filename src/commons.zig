const std = @import("std");
const zdt = @import("zdt");
const StringHashMap = std.StringHashMap;

const Error = error{
    RecordFieldCapacityExceeded,
};

pub const EndpointType = enum {
    Source,
    Sink,
};

pub const FieldType = enum {
    String,
    Int,
    Double,
    TimeStamp,
    Number,
    Boolean,
    Array,
    Map,
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
    TimeStamp: ?zdt.Datetime,
    Number: ?f64,
    Boolean: ?bool,
    Array: ?[]Value,
    Map: ?ValueMap,
    Json: ?ValueMap,

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

pub const ValueMap = struct {
    map: std.StringHashMap(Value),

    pub fn init(allocator: std.mem.Allocator) !ValueMap {
        return .{
            .map = std.StringHashMap(Value).init(allocator),
        };
    }
    pub fn deinit(self: *ValueMap) void {
        self.map.deinit();
    }

    pub fn put(self: *ValueMap, name: []const u8, value: Value) !void {
        try self.map.put(name, value);
    }
    pub fn get(self: ValueMap, name: []const u8) ?Value {
        return self.map.get(name);
    }
    pub fn count(self: ValueMap) usize {
        return self.map.count();
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
    pub fn asMap(self: Record, allocator: std.mem.Allocator, names: []const []const u8) !ValueMap {
        var map = try ValueMap.init(allocator);
        for (names, self.values.items) |name, value| {
            try map.put(name, value);
        }
        return map;
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
