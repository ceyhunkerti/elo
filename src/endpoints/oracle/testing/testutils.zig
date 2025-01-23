const std = @import("std");
const Connection = @import("../Connection.zig");

const Error = error{MissingTestEnvironmentVariable};

pub const ConnectionParams = struct {
    username: []const u8,
    password: []const u8,
    connection_string: []const u8,
    privilege: Connection.Privilege,

    pub fn init() !ConnectionParams {
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
        const auth_mode = std.posix.getenv("ORACLE_TEST_AUTH_MODE") orelse {
            std.debug.print("Missing ORACLE_TEST_AUTH_MODE environment variable\n", .{});
            return error.MissingTestEnvironmentVariable;
        };

        return ConnectionParams{
            .username = username,
            .password = password,
            .connection_string = connection_string,
            .privilege = try Connection.Privilege.fromString(auth_mode),
        };
    }
};
