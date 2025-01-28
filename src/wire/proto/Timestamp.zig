const Timestamp = @This();
const std = @import("std");

pub const TimestampError = error{
    InvalidFormat,
    InvalidMonth,
    InvalidDay,
    InvalidHour,
    InvalidMinute,
    InvalidSecond,
    InvalidTimezone,
};

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

pub fn fromString(str: ?[]const u8) !?Timestamp {
    if (str == null) return null;

    // std.debug.print("Timestamp.fromString: {s}\n", .{str.?});

    const input = std.mem.trim(u8, str.?, &std.ascii.whitespace);
    if (input.len == 0) return null;

    var result = Timestamp{
        .year = 0,
        .month = 1,
        .day = 1,
    };

    // Handle DATE format: YYYY-MM-DD
    if (input.len == 10) {
        try result.parseDatePart(input);
        return result;
    }

    // Handle TIME format: HH:MM:SS[.NNNNNN][+/-HH[:MM]]
    if (input[2] == ':') {
        try result.parseTimePart(input);
        return result;
    }

    // Handle TIMESTAMP/TIMESTAMPTZ format: YYYY-MM-DD HH:MM:SS[.NNNNNN][+/-HH[:MM]]
    if (input.len < 19) return error.InvalidFormat;

    try result.parseDatePart(input[0..10]);
    if (input[10] != ' ') return error.InvalidFormat;
    try result.parseTimePart(input[11..]);

    return result;
}

fn parseDatePart(self: *Timestamp, input: []const u8) !void {
    if (input.len < 10) return error.InvalidFormat;

    self.year = try std.fmt.parseInt(i16, input[0..4], 10);
    if (input[4] != '-') return error.InvalidFormat;

    self.month = try std.fmt.parseInt(u8, input[5..7], 10);
    if (self.month < 1 or self.month > 12) return error.InvalidMonth;
    if (input[7] != '-') return error.InvalidFormat;

    self.day = try std.fmt.parseInt(u8, input[8..10], 10);
    if (self.day < 1 or self.day > 31) return error.InvalidDay;
}

fn parseTimePart(self: *Timestamp, input: []const u8) !void {
    if (input.len < 8) return error.InvalidFormat;

    self.hour = try std.fmt.parseInt(u8, input[0..2], 10);
    if (self.hour > 23) return error.InvalidHour;
    if (input[2] != ':') return error.InvalidFormat;

    self.minute = try std.fmt.parseInt(u8, input[3..5], 10);
    if (self.minute > 59) return error.InvalidMinute;
    if (input[5] != ':') return error.InvalidFormat;

    self.second = try std.fmt.parseInt(u8, input[6..8], 10);
    if (self.second > 59) return error.InvalidSecond;

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
            self.nanosecond = frac * std.math.pow(u32, 10, 9 - @min(frac_str.len, 9));
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

                self.tz_offset.hours = sign * hours;
                self.tz_offset.minutes = sign * minutes;
            },
            else => return error.InvalidFormat,
        }
    }
}

pub fn write(self: Timestamp, result: *std.ArrayList(u8), format: []const u8) !void {
    var i: usize = 0;
    while (i < format.len) {
        const c = format[i];
        if (c == '%' and i + 1 < format.len) {
            i += 1;
            switch (format[i]) {
                'Y' => try std.fmt.formatInt(@abs(self.year), 10, .lower, .{
                    .width = 4,
                    .fill = '0',
                }, result.writer()),
                'y' => try std.fmt.formatInt(@mod(@abs(self.year), 100), 10, .lower, .{ .width = 2, .fill = '0' }, result.writer()),
                'm' => try std.fmt.formatInt(self.month, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer()),
                'd' => try std.fmt.formatInt(self.day, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer()),
                'H' => try std.fmt.formatInt(self.hour, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer()),
                'M' => try std.fmt.formatInt(self.minute, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer()),
                'S' => try std.fmt.formatInt(self.second, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer()),
                'f' => try std.fmt.formatInt(self.nanosecond / std.math.pow(u32, 10, 6), 10, .lower, .{ .width = 3, .fill = '0' }, result.writer()),
                'N' => try std.fmt.formatInt(self.nanosecond, 10, .lower, .{ .width = 9, .fill = '0' }, result.writer()),
                'z' => {
                    // Format timezone offset
                    const abs_hours = @abs(self.tz_offset.hours);
                    try result.append(if (self.tz_offset.hours >= 0) '+' else '-');
                    try std.fmt.formatInt(abs_hours, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer());

                    const abs_minutes = @abs(self.tz_offset.minutes);
                    try std.fmt.formatInt(abs_minutes, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer());
                },
                'Z' => {
                    // Format timezone offset with colon
                    const abs_hours = @abs(self.tz_offset.hours);
                    try result.append(if (self.tz_offset.hours >= 0) '+' else '-');
                    try std.fmt.formatInt(abs_hours, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer());
                    try result.append(':');

                    const abs_minutes = @abs(self.tz_offset.minutes);
                    try std.fmt.formatInt(abs_minutes, 10, .lower, .{ .width = 2, .fill = '0' }, result.writer());
                },
                '%' => try result.append('%'),
                else => {
                    try result.append('%');
                    try result.append(format[i]);
                },
            }
        } else {
            try result.append(c);
        }
        i += 1;
    }
}

pub fn toString(self: Timestamp, allocator: std.mem.Allocator, format: []const u8) ![]const u8 {
    // %Y: Full year (4 digits)
    // %y: Year without century (2 digits)
    // %m: Month (01-12)
    // %d: Day of month (01-31)
    // %H: Hour in 24-hour format (00-23)
    // %M: Minute (00-59)
    // %S: Second (00-59)
    // %f: Milliseconds (3 digits)
    // %N: Nanoseconds (9 digits)
    // %z: Timezone offset in ±HHMM format
    // %Z: Timezone offset in ±HH:MM format
    // %%: Literal percent sign

    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();
    try self.write(&result, format);
    return result.toOwnedSlice();
}
// Example usage:
test "timestamp formatting" {
    const ts = Timestamp{
        .year = 2024,
        .month = 3,
        .day = 14,
        .hour = 15,
        .minute = 9,
        .second = 26,
        .nanosecond = 535000000,
        .tz_offset = .{ .hours = -7, .minutes = 30 },
    };

    const allocator = std.testing.allocator;

    // ISO 8601 format
    const iso = try ts.toString(allocator, "%Y-%m-%dT%H:%M:%S.%f%Z");
    defer allocator.free(iso);
    try std.testing.expectEqualStrings("2024-03-14T15:09:26.535-07:30", iso);

    // Custom format
    const custom = try ts.toString(allocator, "%d/%m/%y %H:%M:%S %z");
    defer allocator.free(custom);
    try std.testing.expectEqualStrings("14/03/24 15:09:26 -0730", custom);

    const just_date = try ts.toString(allocator, "%Y-%m-%d");
    defer allocator.free(just_date);
    try std.testing.expectEqualStrings("2024-03-14", just_date);
}
