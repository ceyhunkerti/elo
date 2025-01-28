const std = @import("std");
const w = @import("../wire.zig");
const M = @import("../M.zig");

const StringHashMap = std.StringHashMap;
const Error = error{
    RecordFieldCapacityExceeded,
};

pub const Timestamp = @import("Timestamp.zig");
pub const Record = @import("Record.zig");
const v = @import("value.zig");
pub const Value = v.Value;
pub const ValueDictionary = v.ValueDictionary;
pub const Field = v.Field;

pub const Metadata = struct {
    name: []const u8,
    fields: []const Field,

    pub fn deinit(self: Metadata, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.fields) |field| field.deinit(allocator);
        allocator.free(self.fields);
    }
};

test {
    std.testing.refAllDecls(@This());
}
