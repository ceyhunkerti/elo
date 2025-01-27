const ConnectionParams = @This();

const Connection = @import("../Connection.zig");
const std = @import("std");

const Error = error{MissingTestEnvironmentVariable};

allocator: std.mem.Allocator,
username: [:0]const u8,
password: [:0]const u8,
host: [:0]const u8,
database: [:0]const u8 = "postgres",

pub fn initFromEnv(allocator: std.mem.Allocator) !ConnectionParams {
    const username = std.posix.getenvZ("PG_TEST_USERNAME") orelse {
        std.debug.print("Missing PG_TEST_USERNAME environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const password = std.posix.getenvZ("PG_TEST_PASSWORD") orelse {
        std.debug.print("Missing PG_TEST_PASSWORD environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const host = std.posix.getenvZ("PG_TEST_HOST") orelse {
        std.debug.print("Missing PG_TEST_HOST environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };
    const database = std.posix.getenvZ("PG_TEST_DATABASE") orelse {
        std.debug.print("Missing PG_TEST_DATABASE environment variable\n", .{});
        return error.MissingTestEnvironmentVariable;
    };

    return ConnectionParams{
        .allocator = allocator,
        .username = username,
        .password = password,
        .host = host,
        .database = database,
    };
}

pub fn toConnection(self: ConnectionParams) Connection {
    return Connection.init(
        self.allocator,
        self.username,
        self.password,
        self.host,
        self.database,
    );
}
