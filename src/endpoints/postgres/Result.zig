const Result = @This();

const std = @import("std");
const c = @import("c.zig").c;

allocator: std.mem.Allocator,

pg_result: ?*c.PGresult = null,

pub fn init(allocator: std.mem.Allocator, pg_result: ?*c.PGresult) Result {
    return .{
        .allocator = allocator,
        .pg_result = pg_result,
    };
}
pub fn deinit(self: Result) void {
    c.PQclear(self.pg_result);
}
