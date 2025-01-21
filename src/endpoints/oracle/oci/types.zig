const std = @import("std");

pub const bytes = struct {
    ptr: [*c]u8 = std.mem.zeroes([*c]u8),
    length: u32 = std.mem.zeroes(u32),
    encoding: [*c]const u8 = std.mem.zeroes([*c]const u8),
};

pub const QueryValue = union(enum) {
    boolean: ?c_int,
    uint8: ?u8,
    uint16: ?u16,
    uint32: ?u32,
    uint64: ?u64,
    int8: ?i8,
    int16: ?i16,
    int32: ?i32,
    int64: ?i64,
    float32: ?f32,
    float64: ?f64,
    string: ?[*:0]const u8,
    bytes: ?*bytes,
    // todo
};
