const std = @import("std");
const Timestamp = @import("Timestamp.zig");
const p = @import("proto.zig");

pub const FieldType = enum {
    String,
    Int,
    Double,
    TimeStamp,
    Boolean,
    Array,
    Json,
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

    pub fn write(self: Value, result: *std.ArrayList(u8), formatter: p.ValueFormatter) !void {
        switch (self) {
            .String => |str| if (str) |s| try result.appendSlice(s) else try result.appendSlice(""),
            .Int => |num| if (num) |n| try result.writer().print("{d}", .{n}) else try result.appendSlice(""),
            .Double => |num| if (num) |n| try result.writer().print("{d}", .{n}) else try result.appendSlice(""),
            .Boolean => |boolean| if (boolean) |b| try result.append(if (b) '1' else '0') else try result.append('0'),
            .TimeStamp => |timestamp| if (timestamp) |t| try t.write(result, formatter.time_format) else try result.appendSlice(""),
            else => {
                std.debug.print("Unsupported type: {s}\n", .{@tagName(self)});
                return error.UnsupportedType;
            },
            // .Array => |arr| if (arr) |a| {
            //     for (a) |item| try item.write(result);
            // },
            // .Json => |json| if (json) |j| try j.write(result),
        }
    }

    // returned memory is owned by the caller
    pub fn toString(self: Value, allocator: std.mem.Allocator, formatter: p.ValueFormatter) ![]const u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();
        try self.write(&result, formatter);
        return result.toOwnedSlice();
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
