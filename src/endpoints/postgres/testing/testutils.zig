const std = @import("std");
const Connection = @import("../Connection.zig");
pub const Error = error{MissingTestEnvironmentVariable};

pub const ConnectionParams = struct {
    username: []const u8,
    password: []const u8,
    host: []const u8,
    database: []const u8 = "postgres",
};

pub fn connectionParams() !ConnectionParams {
    const username = std.posix.getenv("PG_TEST_USERNAME") orelse {
        std.debug.print("Missing PG_TEST_USERNAME environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const password = std.posix.getenv("PG_TEST_PASSWORD") orelse {
        std.debug.print("Missing PG_TEST_PASSWORD environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const host = std.posix.getenv("PG_TEST_HOST") orelse {
        std.debug.print("Missing PG_TEST_HOST environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const database = std.posix.getenv("PG_TEST_DATABASE") orelse {
        std.debug.print("Missing PG_TEST_DATABASE environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };

    return .{
        .username = username,
        .password = password,
        .host = host,
        .database = database,
    };
}

pub fn connection(allocator: std.mem.Allocator) !Connection {
    const params = try connectionParams();
    return Connection.init(allocator, params.username, params.password, params.host, params.database);
}
