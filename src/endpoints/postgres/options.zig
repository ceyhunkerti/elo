const std = @import("std");
const c = @import("c.zig").c;
const CopyOptions = @import("Copy.zig").Options;

const Error = error{
    HostNotFound,
    DatabaseNotFound,
    UsernameNotFound,
    PasswordNotFound,
    TableNameNotFound,
    InvalidBatchSize,
    InvalidFetchSize,
    InvalidCopyOptions,
    FailedToDetectMode,
    SqlNotFound,
};

pub const SinkMode = enum { Append, Truncate };

pub const ConnectionOptions = struct {
    host: [:0]const u8,
    database: [:0]const u8,
    username: [:0]const u8,
    password: [:0]const u8,

    pub fn fromMap(allocator: std.mem.Allocator, map: std.StringHashMap([]const u8)) !ConnectionOptions {
        const host = map.get("host") orelse return error.HostNotFound;
        const database = map.get("database") orelse return error.DatabaseNotFound;
        const username = map.get("username") orelse return error.UsernameNotFound;
        const password = map.get("password") orelse return error.PasswordNotFound;

        return .{
            .host = try allocator.dupeZ(u8, host),
            .database = try allocator.dupeZ(u8, database),
            .username = try allocator.dupeZ(u8, username),
            .password = try allocator.dupeZ(u8, password),
        };
    }

    pub fn deinit(self: ConnectionOptions, allocator: std.mem.Allocator) void {
        allocator.free(self.host);
        allocator.free(self.database);
        allocator.free(self.username);
        allocator.free(self.password);
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try output.appendSlice(
            \\Connection options:
            \\
            \\--host [REQUIRED]
            \\
            \\--database [REQUIRED]
            \\
            \\--username [REQUIRED]
            \\
            \\--password [REQUIRED]
            \\
        );
    }
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    sql: [:0]const u8,
    fetch_size: u32 = 10_000,

    pub fn fromMap(allocator: std.mem.Allocator, map: std.StringHashMap([]const u8)) !SourceOptions {
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
            .sql = try allocator.dupeZ(u8, sql),
        };
    }

    pub fn deinit(self: SourceOptions, allocator: std.mem.Allocator) void {
        allocator.free(self.sql);
        self.connection.deinit(allocator);
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try ConnectionOptions.help(output);
        try output.appendSlice(
            \\
            \\--sql [REQUIRED]
            \\
            \\--fetch_size [OPTIONAL - defaults to 10_000]
            \\
        );
    }
};

pub const SinkOptions = struct {
    connection: ConnectionOptions,
    columns: ?[]const []const u8 = null,
    table: []const u8,
    mode: SinkMode = .Append,
    copy_options: ?CopyOptions = null,
    batch_size: u32 = 10_000,

    pub fn fromMap(allocator: std.mem.Allocator, map: std.StringHashMap([]const u8)) !SinkOptions {
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
        const copy_options = try CopyOptions.fromMap(allocator, map);
        const connection = try ConnectionOptions.fromMap(allocator, map);

        return .{
            .connection = connection,
            .columns = columns,
            .table = table,
            .mode = mode,
            .copy_options = copy_options,
            .batch_size = batch_size,
        };
    }

    pub fn deinit(self: SinkOptions, allocator: std.mem.Allocator) void {
        self.connection.deinit(allocator);
        if (self.columns) |columns| {
            for (columns) |column| {
                allocator.free(column);
            }
            allocator.free(columns);
        }
        if (self.copy_options) |copy_options| {
            copy_options.deinit(allocator);
        }
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try ConnectionOptions.help(output);
        try output.appendSlice(
            \\General options:
            \\--table [REQUIRED]
            \\
            \\--mode [OPTIONAL]
            \\
            \\--batch_size [OPTIONAL]
            \\
            \\--columns [OPTIONAL]
            \\
        );
        try CopyOptions.help(output);
    }
};
