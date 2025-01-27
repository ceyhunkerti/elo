const Column = @This();
const std = @import("std");
const pgtype = @import("pgtype.zig");
const c = @import("../c.zig").c;

const Error = error{
    TypeError,
};

allocator: std.mem.Allocator,
index: i32,
name: []const u8 = undefined,
type: pgtype.PostgresType = undefined,

nullable: bool = true,
length: u32 = 0,
precision: ?u32 = null,
scale: ?u32 = null,
default: ?[]const u8 = null,

pub fn deinit(self: Column) void {
    self.allocator.free(self.name);
}

pub fn fromPGMetadata(
    allocator: std.mem.Allocator,
    res: ?*const c.PGresult,
    index: i32,
) !Column {
    const type_oid = c.PQftype(res, @intCast(index));
    const column_name = std.mem.span(c.PQfname(res, @intCast(index)));

    return .{
        .allocator = allocator,
        .index = index,
        .name = try allocator.dupe(u8, column_name),
        .type = pgtype.PostgresType.fromOid(type_oid) orelse {
            std.debug.print("Error: could not find type for OID {d}\n", .{type_oid});
            return error.TypeError;
        },
    };
}
