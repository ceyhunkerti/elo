const std = @import("std");
const zdt = @import("zdt");
const StringHashMap = std.StringHashMap;

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

pub const Field = struct {
    index: usize,
    name: ?[]const u8 = null,
    type: ?FieldType = null,
    default: ?FieldValue = null,
    description: ?[]const u8 = null,
    nullable: bool = true,
    length: ?u32 = null,
    precision: ?u8 = null,
    scale: ?u8 = null,
};

pub const Metadata = struct {
    name: []const u8,
    fields: []const Field,
};

pub const FieldValue = union(FieldType) {
    String: ?[]u8,
    Int: ?i64,
    Double: ?f64,
    TimeStamp: ?zdt.Datetime,
    Number: ?f64,
    Boolean: ?bool,
    Array: ?[]FieldValue,
    Map: ?StringHashMap(FieldValue),
    Json: ?StringHashMap(FieldValue),

    pub fn deinit(self: FieldValue, allocator: std.mem.Allocator) void {
        switch (self) {
            .String => |str| if (str) |s| allocator.free(s),
            // todo
            else => {},
        }
    }
};

pub const Record = []FieldValue;

pub fn deinitRecord(allocator: std.mem.Allocator, record: Record) void {
    for (record) |value| value.deinit(allocator);
    allocator.free(record);
}

test "deinitRecord" {
    const allocator = std.testing.allocator;
    var record = try allocator.alloc(FieldValue, 2);
    record[0] = FieldValue{ .Int = 1 };
    record[1] = FieldValue{ .String = try allocator.dupe(u8, "hello") };
    deinitRecord(allocator, record);
}

pub fn RecordAsMap(allocator: std.mem.Allocator, names: []const []const u8, record: Record) StringHashMap(FieldValue) {
    var map = StringHashMap(FieldValue).init(allocator);
    for (names, record) |name, value| {
        map.put(name, value) catch unreachable;
    }
    return map;
}
