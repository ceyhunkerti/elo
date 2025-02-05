const std = @import("std");
const c = @import("c.zig").c;
const Privilege = @import("./Connection.zig").Privilege;
const StringMap = std.StringHashMap([]const u8);

pub const Error = error{
    ConnectionStringNotFound,
    UsernameNotFound,
    PasswordNotFound,
    FailedToDetectPrivilege,
    TableNameNotFound,
    FailedToDetectMode,
    InvalidBatchSize,
    InvalidFetchSize,
    SqlNotFound,
};

pub const ConnectionOptions = struct {
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
    privilege: Privilege = .DEFAULT,

    pub fn fromStringMap(map: StringMap) !ConnectionOptions {
        const connection_string = map.get("connection-string") orelse {
            return Error.ConnectionStringNotFound;
        };
        const username = map.get("username") orelse {
            return Error.UsernameNotFound;
        };
        const password = map.get("password") orelse {
            return Error.PasswordNotFound;
        };
        const privilege = if (map.get("privilege")) |priv| Privilege.fromString(priv) catch {
            return Error.FailedToDetectPrivilege;
        } else Privilege.DEFAULT;

        return .{
            .connection_string = connection_string,
            .username = username,
            .password = password,
            .privilege = privilege,
        };
    }
};
test "ConnectionOptions.fromStringMap" {
    const allocator = std.testing.allocator;
    var map = StringMap.init(allocator);
    defer map.deinit();
    try map.put("connection-string", "foo");
    try map.put("username", "bar");
    try map.put("password", "baz");
    try map.put("privilege", "SYSDBA");
    const opts = try ConnectionOptions.fromStringMap(map);
    try std.testing.expectEqualStrings("foo", opts.connection_string);
    try std.testing.expectEqualStrings("bar", opts.username);
    try std.testing.expectEqualStrings("baz", opts.password);
    try std.testing.expectEqual(Privilege.SYSDBA, opts.privilege);
}

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u32 = 10_000,
    sql: []const u8,

    pub fn fromStringMap(allocator: std.mem.Allocator, map: StringMap) !SourceOptions {
        var conn_map = StringMap.init(allocator);
        defer conn_map.deinit();
        var it = map.iterator();
        while (it.next()) |kv| {
            if (std.mem.startsWith(u8, kv.key_ptr.*, "conn--")) {
                try conn_map.put(kv.key_ptr.*[6..], kv.value_ptr.*);
            }
        }
        const sql = map.get("sql") orelse {
            return Error.SqlNotFound;
        };
        const fetch_size = fetch_size: {
            if (map.get("fetch_size")) |bs| {
                break :fetch_size std.fmt.parseInt(u32, bs, 10) catch {
                    return Error.InvalidFetchSize;
                };
            }
            break :fetch_size 10_000;
        };

        const connection = try ConnectionOptions.fromStringMap(conn_map);

        return .{
            .connection = connection,
            .fetch_size = fetch_size,
            .sql = sql,
        };
    }
};

test "SourceOptions.fromStringMap" {
    const allocator = std.testing.allocator;
    var map = StringMap.init(allocator);
    defer map.deinit();
    try map.put("conn--connection-string", "foo");
    try map.put("conn--host", "foo");
    try map.put("conn--username", "bar");
    try map.put("conn--password", "baz");
    try map.put("sql", "foo");
    const opts = try SourceOptions.fromStringMap(allocator, map);
    try std.testing.expectEqualStrings("foo", opts.connection.connection_string);
    try std.testing.expectEqualStrings("bar", opts.connection.username);
    try std.testing.expectEqualStrings("baz", opts.connection.password);
    try std.testing.expectEqualStrings("foo", opts.sql);
    try std.testing.expectEqual(10_000, opts.fetch_size);
}

pub const SinkMode = enum { Append, Truncate };

pub const SinkOptions = struct {
    allocator: std.mem.Allocator = undefined,
    connection: ConnectionOptions,
    table: []const u8,
    columns: ?[][]const u8 = null,
    sql: ?[]const u8 = null,
    mode: SinkMode = .Append,
    batch_size: u32 = 10_000,

    pub fn fromStringMap(allocator: std.mem.Allocator, map: StringMap) !SinkOptions {
        var conn_map = StringMap.init(allocator);
        defer conn_map.deinit();
        var it = map.iterator();
        while (it.next()) |kv| {
            if (std.mem.startsWith(u8, kv.key_ptr.*, "conn--")) {
                try conn_map.put(kv.key_ptr.*[6..], kv.value_ptr.*);
            }
        }
        const table = map.get("table") orelse {
            return Error.TableNameNotFound;
        };
        const mode = mode: {
            if (map.get("mode")) |mode_str| {
                break :mode std.meta.stringToEnum(SinkMode, mode_str) orelse {
                    return Error.FailedToDetectMode;
                };
            } else break :mode .Append;
        };
        const batch_size = batch_size: {
            if (map.get("batch_size")) |bs| {
                break :batch_size std.fmt.parseInt(u32, bs, 10) catch {
                    return Error.InvalidBatchSize;
                };
            } else break :batch_size 10_000;
        };

        const connection = try ConnectionOptions.fromStringMap(conn_map);

        return .{
            .allocator = allocator,
            .connection = connection,
            .table = table,
            .mode = mode,
            .batch_size = batch_size,
        };
    }
};

test "SinkOptions.fromStringMap" {
    const allocator = std.testing.allocator;
    var map = StringMap.init(allocator);
    defer map.deinit();
    try map.put("conn--connection-string", "foo");
    try map.put("conn--host", "foo");
    try map.put("conn--username", "bar");
    try map.put("conn--password", "baz");
    try map.put("table", "foo");
    const opts = try SinkOptions.fromStringMap(allocator, map);
    try std.testing.expectEqualStrings("foo", opts.connection.connection_string);
    try std.testing.expectEqualStrings("bar", opts.connection.username);
    try std.testing.expectEqualStrings("baz", opts.connection.password);
    try std.testing.expectEqualStrings("foo", opts.table);
    try std.testing.expectEqual(.Append, opts.mode);
    try std.testing.expectEqual(10_000, opts.batch_size);
}
