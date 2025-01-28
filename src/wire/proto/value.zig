const std = @import("std");
const Timestamp = @import("Timestamp.zig");

pub const FormatError = error{
    UnsupportedType,
};

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

    // returned memory is owned by the caller
    pub fn toString(self: Value, allocator: std.mem.Allocator) ![]u8 {
        return switch (self) {
            .String => |str| if (str) |s| try allocator.dupe(u8, s) else try allocator.dupe(u8, ""),
            .Double, .Int => |num| if (num) |n| try std.fmt.allocPrint(allocator, "{d}", .{n}) else try allocator.dupe(u8, ""),
            .Boolean => |boolean| if (boolean) |b| try std.fmt.allocPrint(allocator, "{b}", .{b}) else try allocator.dupe(u8, ""),
            else => {
                std.debug.print("Unsupported type: {s}\n", .{@tagName(self)});
                return error.UnsupportedType;
            },
        };
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
