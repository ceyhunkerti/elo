const std = @import("std");

const ConnectionOptions = @import("../options.zig").ConnectionOptions;
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

pub fn connectionParams(allocator: std.mem.Allocator) ConnectionParams {
    return ConnectionParams.initFromEnv(allocator) catch unreachable;
}

pub fn connectionOptions(allocator: std.mem.Allocator) ConnectionOptions {
    const tp = connectionParams(allocator);
    return .{
        .username = tp.username,
        .password = tp.password,
        .connection_string = tp.connection_string,
        .privilege = tp.privilege,
    };
}
