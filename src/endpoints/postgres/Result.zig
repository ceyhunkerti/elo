const std = @import("std");
const c = @import("c.zig").c;

const Self = @This();

allocator: std.mem.Allocator,

pg_result: ?*c.PGresult = null,
