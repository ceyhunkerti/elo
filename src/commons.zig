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
};

pub const Record = []FieldValue;
