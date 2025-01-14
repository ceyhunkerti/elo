const std = @import("std");

const c = @import("../c.zig").c;
const Connection = @import("../Connection.zig");

const TestConnectionError = error{MissingTestEnvironmentVariable};

pub const TestConnectionParams = struct {
    username: []const u8,
    password: []const u8,
    connection_string: []const u8,
    privilege: Connection.Privilege,
};

pub fn getTestConnectionParams() !TestConnectionParams {
    const username = std.posix.getenv("ORACLE_TEST_USERNAME") orelse {
        std.debug.print("Missing ORACLE_TEST_USERNAME environment variable\n", .{});
        return TestConnectionError.MissingTestEnvironmentVariable;
    };
    const password = std.posix.getenv("ORACLE_TEST_PASSWORD") orelse {
        std.debug.print("Missing ORACLE_TEST_PASSWORD environment variable\n", .{});
        return TestConnectionError.MissingTestEnvironmentVariable;
    };
    const connection_string = std.posix.getenv("ORACLE_TEST_CONNECTION_STRING") orelse {
        std.debug.print("Missing ORACLE_TEST_CONNECTION_STRING environment variable\n", .{});
        return TestConnectionError.MissingTestEnvironmentVariable;
    };
    const auth_mode = std.posix.getenv("ORACLE_TEST_AUTH_MODE") orelse {
        std.debug.print("Missing ORACLE_TEST_AUTH_MODE environment variable\n", .{});
        return TestConnectionError.MissingTestEnvironmentVariable;
    };

    return TestConnectionParams{
        .username = username,
        .password = password,
        .connection_string = connection_string,
        .privilege = try Connection.Privilege.fromString(auth_mode),
    };
}

pub fn getTestConnection(allocator: std.mem.Allocator) !Connection {
    const params = try getTestConnectionParams();
    return Connection.init(
        allocator,
        params.username,
        params.password,
        params.connection_string,
        params.privilege,
    );
}
