const ConnectionParams = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");

pub const Error = error{MissingTestEnvironmentVariable};

allocator: std.mem.Allocator,
username: []const u8,
password: []const u8,
connection_string: []const u8,
privilege: Connection.Privilege,

pub fn initFromEnv(allocator: std.mem.Allocator) !ConnectionParams {
    const username = std.posix.getenv("ORACLE_TEST_USERNAME") orelse {
        std.debug.print("Missing ORACLE_TEST_USERNAME environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const password = std.posix.getenv("ORACLE_TEST_PASSWORD") orelse {
        std.debug.print("Missing ORACLE_TEST_PASSWORD environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const connection_string = std.posix.getenv("ORACLE_TEST_CONNECTION_STRING") orelse {
        std.debug.print("Missing ORACLE_TEST_CONNECTION_STRING environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const auth_mode = std.posix.getenv("ORACLE_TEST_AUTH_MODE");

    return ConnectionParams{
        .allocator = allocator,
        .username = username,
        .password = password,
        .connection_string = connection_string,
        .privilege = try Connection.Privilege.fromString(auth_mode),
    };
}

pub fn toConnection(self: ConnectionParams) Connection {
    return Connection.init(
        self.allocator,
        self.username,
        self.password,
        self.connection_string,
        self.privilege,
    );
}
