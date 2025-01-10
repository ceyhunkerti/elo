const std = @import("std");
const zdt = @import("zdt");
const StringHashMap = std.StringHashMap;

pub const ParameterValue = union(enum) {
    String: []const u8,
    Int: i64,
    Double: f64,
    TimeStamp: zdt.Datetime,
    Number: f64,
    Boolean: bool,
    Array: []ParameterValue,
    Map: StringHashMap(ParameterValue),
    Json: StringHashMap(ParameterValue),
    Nil,
};

pub const Parameter = struct {
    name: []const u8,
    required: bool,
    description: []const u8,
    typestr: []const u8,
    default_value: ?ParameterValue = null,
};
