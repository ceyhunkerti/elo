const std = @import("std");

const Connection = @import("../Connection.zig");
pub const TestTable = @import("./TestTable.zig");
pub const ConnectionParams = @import("./ConnectionParams.zig");

pub fn connection(allocator: std.mem.Allocator) Connection {
    const tp = ConnectionParams.initFromEnv(allocator) catch unreachable;
    return tp.toConnection();
}

pub fn schema() []const u8 {
    const p = ConnectionParams.initFromEnv(std.testing.allocator) catch unreachable;
    return p.username;
}

pub fn connectionParams(allocator: std.mem.Allocator) !ConnectionParams {
    return ConnectionParams.initFromEnv(allocator) catch unreachable;
}
