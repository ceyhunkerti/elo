const std = @import("std");
const c = @import("../c.zig").c;
const p = @import("../../../wire/proto/proto.zig");

const Error = error{invalidFormat};

pub const PostgresType = enum(c.Oid) {
    // Boolean
    bool = 16,

    // Numbers
    int2 = 21, // smallint
    int4 = 23, // integer
    int8 = 20, // bigint
    float4 = 700, // real
    float8 = 701, // double precision
    numeric = 1700,
    money = 790,

    // Character types
    char = 18,
    name = 19,
    text = 25,
    varchar = 1043,
    bpchar = 1042, // char(n)

    // Binary data
    bytea = 17,

    // Date/Time
    date = 1082,
    time = 1083,
    timetz = 1266,
    timestamp = 1114,
    timestamptz = 1184,
    interval = 1186,

    // Network
    inet = 869,
    cidr = 650,
    macaddr = 829,
    macaddr8 = 774,

    // Geometric
    point = 600,
    line = 628,
    lseg = 601,
    box = 603,
    path = 602,
    polygon = 604,
    circle = 718,

    // Arrays
    bool_array = 1000,
    int2_array = 1005,
    int4_array = 1007,
    int8_array = 1016,
    float4_array = 1021,
    float8_array = 1022,
    text_array = 1009,
    varchar_array = 1015,

    // JSON
    json = 114,
    jsonb = 3802,

    // UUID
    uuid = 2950,

    // Range types
    int4range = 3904,
    int8range = 3926,
    numrange = 3906,
    tsrange = 3908,
    tstzrange = 3910,
    daterange = 3912,

    pub fn fromOid(oid: c.Oid) ?PostgresType {
        return std.meta.intToEnum(PostgresType, oid) catch null;
    }

    pub fn toZigType(self: PostgresType) type {
        return switch (self) {
            .bool => bool,
            .int2 => i16,
            .int4 => i32,
            .int8 => i64,
            .float4 => f32,
            .float8 => f64,
            .text, .varchar, .bpchar, .char, .name => []const u8,
            .bytea => []const u8,
            .date, .time, .timetz, .timestamp, .timestamptz, .interval => i64,
            .uuid => [16]u8,
            .json, .jsonb => []const u8,
            else => []const u8, // Default to string for unsupported types
        };
    }

    pub fn isArray(self: PostgresType) bool {
        return switch (self) {
            .bool_array, .int2_array, .int4_array, .int8_array, .float4_array, .float8_array, .text_array, .varchar_array => true,
            else => false,
        };
    }

    pub inline fn stringToValue(self: PostgresType, allocator: std.mem.Allocator, str: ?[]const u8) p.Value {
        return switch (self) {
            .bool => p.Value{ .Boolean = if (str) |s| std.mem.eql(u8, s, "t") else null },
            .int2, .int4, .int8 => p.Value{ .Int = if (str) |s| std.fmt.parseInt(i64, s, 10) catch unreachable else null },
            .numeric, .float4, .float8 => p.Value{ .Double = if (str) |s| std.fmt.parseFloat(f64, s) catch unreachable else null },
            .text => p.Value{ .Bytes = if (str) |s| allocator.dupe(u8, s) catch unreachable else null },
            .timestamp, .date, .timetz, .timestamptz => p.Value{ .TimeStamp = p.Timestamp.fromString(str) catch unreachable },
            else => p.Value{ .Bytes = if (str) |s| allocator.dupe(u8, s) catch unreachable else null },
        };
    }
};

const Timestamp = p.Timestamp;

pub fn timestampFromString(str: ?[]const u8) !?Timestamp {
    if (str == null) return null;
    const input = std.mem.trim(u8, str.?, &std.ascii.whitespace);
    if (input.len == 0) return null;

    var result = Timestamp{
        .year = 0,
        .month = 1,
        .day = 1,
    };

    // Handle DATE format: YYYY-MM-DD
    if (input.len == 10) {
        try parseDatePart(input, &result);
        return result;
    }

    // Handle TIME format: HH:MM:SS[.NNNNNN][+/-HH[:MM]]
    if (input[2] == ':') {
        try parseTimePart(input, &result);
        return result;
    }

    // Handle TIMESTAMP/TIMESTAMPTZ format: YYYY-MM-DD HH:MM:SS[.NNNNNN][+/-HH[:MM]]
    if (input.len < 19) return error.InvalidFormat;

    try parseDatePart(input[0..10], &result);
    if (input[10] != ' ') return error.InvalidFormat;
    try parseTimePart(input[11..], &result);

    return result;
}

fn parseDatePart(input: []const u8, result: *Timestamp) !void {
    if (input.len < 10) return error.InvalidFormat;

    result.year = try std.fmt.parseInt(i16, input[0..4], 10);
    if (input[4] != '-') return error.InvalidFormat;

    result.month = try std.fmt.parseInt(u8, input[5..7], 10);
    if (result.month < 1 or result.month > 12) return error.InvalidMonth;
    if (input[7] != '-') return error.InvalidFormat;

    result.day = try std.fmt.parseInt(u8, input[8..10], 10);
    if (result.day < 1 or result.day > 31) return error.InvalidDay;
}

fn parseTimePart(input: []const u8, result: *Timestamp) !void {
    if (input.len < 8) return error.InvalidFormat;

    result.hour = try std.fmt.parseInt(u8, input[0..2], 10);
    if (result.hour > 23) return error.InvalidHour;
    if (input[2] != ':') return error.InvalidFormat;

    result.minute = try std.fmt.parseInt(u8, input[3..5], 10);
    if (result.minute > 59) return error.InvalidMinute;
    if (input[5] != ':') return error.InvalidFormat;

    result.second = try std.fmt.parseInt(u8, input[6..8], 10);
    if (result.second > 59) return error.InvalidSecond;

    var pos: usize = 8;

    // Parse optional fractional seconds
    if (pos < input.len and input[pos] == '.') {
        pos += 1;
        const frac_start = pos;
        while (pos < input.len and std.ascii.isDigit(input[pos])) : (pos += 1) {}
        const frac_str = input[frac_start..pos];

        if (frac_str.len > 0) {
            const frac = try std.fmt.parseInt(u32, frac_str, 10);
            // Convert to nanoseconds (padding with zeros if less than 9 digits)
            result.nanosecond = frac * std.math.pow(u32, 10, 9 - @min(frac_str.len, 9));
        }
    }

    // Parse optional timezone
    if (pos < input.len) {
        switch (input[pos]) {
            '+', '-' => {
                const sign: i8 = if (input[pos] == '+') 1 else -1;
                pos += 1;

                if (pos + 2 > input.len) return error.InvalidFormat;
                const hours = try std.fmt.parseInt(i8, input[pos .. pos + 2], 10);
                if (hours > 23) return error.InvalidTimezone;
                pos += 2;

                var minutes: i8 = 0;
                if (pos < input.len) {
                    if (input[pos] == ':') pos += 1;
                    if (pos + 2 > input.len) return error.InvalidFormat;
                    minutes = try std.fmt.parseInt(i8, input[pos .. pos + 2], 10);
                    if (minutes > 59) return error.InvalidTimezone;
                }

                result.tz_offset.hours = sign * hours;
                result.tz_offset.minutes = sign * minutes;
            },
            else => return error.InvalidFormat,
        }
    }
}
