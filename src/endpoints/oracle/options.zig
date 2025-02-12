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

    pub fn fromMap(allocator: std.mem.Allocator, map: StringMap) !ConnectionOptions {
        const connection_string = map.get("connection-string") orelse return Error.ConnectionStringNotFound;
        const username = map.get("username") orelse return Error.UsernameNotFound;
        const password = map.get("password") orelse return Error.PasswordNotFound;
        const privilege = if (map.get("privilege")) |priv| Privilege.fromString(priv) catch {
            return Error.FailedToDetectPrivilege;
        } else Privilege.DEFAULT;

        return .{
            .connection_string = try allocator.dupe(u8, connection_string),
            .username = try allocator.dupe(u8, username),
            .password = try allocator.dupe(u8, password),
            .privilege = privilege,
        };
    }

    pub fn deinit(self: ConnectionOptions, allocator: std.mem.Allocator) void {
        allocator.free(self.connection_string);
        allocator.free(self.username);
        allocator.free(self.password);
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try output.appendSlice(
            \\--connection-string [REQUIRED]
            \\  Oracle connection string  <host>:<port>/<SID|SERVICE_NAME|PDB>
            \\  Example: somedb99.example.com:1234/orclpdb
            \\
            \\--username [REQUIRED]
            \\
            \\--password [REQUIRED]
            \\
            \\--privilege [OPTIONAL]
            \\  Oracle privilege  SYSDBA|SYSOPER|...|DEFAULT
        );
    }
};
test "ConnectionOptions.fromMap" {
    const allocator = std.testing.allocator;
    var map = StringMap.init(allocator);
    defer map.deinit();
    try map.put("connection-string", "foo");
    try map.put("username", "bar");
    try map.put("password", "baz");
    try map.put("privilege", "SYSDBA");
    const opts = try ConnectionOptions.fromMap(allocator, map);
    defer opts.deinit(allocator);
    try std.testing.expectEqualStrings("foo", opts.connection_string);
    try std.testing.expectEqualStrings("bar", opts.username);
    try std.testing.expectEqualStrings("baz", opts.password);
    try std.testing.expectEqual(Privilege.SYSDBA, opts.privilege);
}

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u32 = 10_000,
    sql: []const u8,

    pub fn fromMap(allocator: std.mem.Allocator, map: StringMap) !SourceOptions {
        const sql = map.get("sql") orelse return Error.SqlNotFound;
        const fetch_size = fetch_size: {
            if (map.get("fetch_size")) |bs| {
                break :fetch_size std.fmt.parseInt(u32, bs, 10) catch {
                    return Error.InvalidFetchSize;
                };
            }
            break :fetch_size 10_000;
        };
        const connection = try ConnectionOptions.fromMap(allocator, map);

        return .{
            .connection = connection,
            .fetch_size = fetch_size,
            .sql = try allocator.dupe(u8, sql),
        };
    }

    pub fn deinit(self: SourceOptions, allocator: std.mem.Allocator) void {
        allocator.free(self.sql);
        self.connection.deinit(allocator);
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try ConnectionOptions.help(output);

        try output.appendSlice(
            \\--sql [REQUIRED]
            \\  SQL query to execute
            \\
            \\--fetch-size [OPTIONAL]
            \\  Fetch size for the query defaults to 10_000
        );
    }
};

test "SourceOptions.fromMap" {
    const allocator = std.testing.allocator;
    var map = StringMap.init(allocator);
    defer map.deinit();
    try map.put("connection-string", "foo");
    try map.put("host", "foo");
    try map.put("username", "bar");
    try map.put("password", "baz");
    try map.put("sql", "foo");
    const opts = try SourceOptions.fromMap(allocator, map);
    defer opts.deinit(allocator);
    try std.testing.expectEqualStrings("foo", opts.connection.connection_string);
    try std.testing.expectEqualStrings("bar", opts.connection.username);
    try std.testing.expectEqualStrings("baz", opts.connection.password);
    try std.testing.expectEqualStrings("foo", opts.sql);
    try std.testing.expectEqual(10_000, opts.fetch_size);
}

pub const SinkMode = enum { Append, Truncate };

pub const SinkOptions = struct {
    connection: ConnectionOptions,
    table: []const u8,
    columns: ?[][]const u8 = null,
    sql: ?[]const u8 = null,
    mode: SinkMode = .Append,
    batch_size: u32 = 10_000,

    pub fn fromMap(allocator: std.mem.Allocator, map: StringMap) !SinkOptions {
        const table = map.get("table") orelse return Error.TableNameNotFound;
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

        const connection = try ConnectionOptions.fromMap(allocator, map);

        const columns = columns: {
            if (map.get("columns")) |columns_str| {
                var result = std.ArrayList([]const u8).init(allocator);
                defer result.deinit();

                var it = std.mem.split(u8, columns_str, ",");
                while (it.next()) |column| {
                    try result.append(column);
                }
                break :columns try result.toOwnedSlice();
            } else break :columns null;
        };

        return .{
            .connection = connection,
            .table = try allocator.dupe(u8, table),
            .columns = columns,
            .mode = mode,
            .batch_size = batch_size,
        };
    }

    pub fn deinit(self: SinkOptions, allocator: std.mem.Allocator) void {
        allocator.free(self.table);
        if (self.columns) |columns| {
            for (columns) |column| {
                allocator.free(column);
            }
            allocator.free(columns);
        }
        self.connection.deinit(allocator);
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try ConnectionOptions.help(output);

        try output.appendSlice(
            \\--table [REQUIRED]
            \\  Table to write
            \\
            \\--mode [OPTIONAL]
            \\  Mode to write to defaults to Append
            \\  Possible values are: Append, Truncate
            \\
            \\--batch-size [OPTIONAL]
            \\  Batch size for the query defaults to 10_000
            \\
        );
    }
};

test "SinkOptions.fromMap" {
    const allocator = std.testing.allocator;
    var map = StringMap.init(allocator);
    defer map.deinit();
    try map.put("connection-string", "foo");
    try map.put("host", "foo");
    try map.put("username", "bar");
    try map.put("password", "baz");
    try map.put("table", "foo");
    const opts = try SinkOptions.fromMap(allocator, map);
    defer opts.deinit(allocator);
    try std.testing.expectEqualStrings("foo", opts.connection.connection_string);
    try std.testing.expectEqualStrings("bar", opts.connection.username);
    try std.testing.expectEqualStrings("baz", opts.connection.password);
    try std.testing.expectEqualStrings("foo", opts.table);
    try std.testing.expectEqual(.Append, opts.mode);
    try std.testing.expectEqual(10_000, opts.batch_size);
}
