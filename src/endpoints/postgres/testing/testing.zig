const std = @import("std");
const Connection = @import("../Connection.zig");

pub const ConnectionParams = @import("./ConnectionParams.zig");

pub fn connectionParams(allocator: std.mem.Allocator) ConnectionParams {
    return ConnectionParams.initFromEnv(allocator) catch unreachable;
}
pub fn connection(allocator: std.mem.Allocator) Connection {
    return connectionParams(allocator).toConnection();
}
