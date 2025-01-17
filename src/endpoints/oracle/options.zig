const std = @import("std");
const c = @import("c.zig").c;
const Privilege = @import("./Connection.zig").Privilege;
const metadata = @import("./metadata/metadata.zig");

pub const ConnectionOptions = struct {
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
    privilege: Privilege = .DEFAULT,
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u32 = 10_000,
    sql: []const u8,

    // pub fn validate() !void {} // TODO
};

pub const Column = struct {
    name: []const u8,
    type: []const u8,
    length: ?u32 = null,
    precision: ?u32 = null,
    scale: ?u32 = null,
    default: ?[]const u8 = null,
    nullable: bool = true,
};

pub const SinkOptions = struct {
    allocator: std.mem.Allocator = undefined,
    connection: ConnectionOptions,
    table: []const u8,
    columns: ?[]const Column = null,
    sql: ?[]const u8 = null,
    mode: enum { Append, Truncate } = .Append,
    batch_size: u32 = 10_000,

    pub fn columnNames(self: SinkOptions) ?[]const []const u8 {
        if (self.columns) |cols| {
            var names = std.ArrayList([]const u8).init(self.allocator);
            for (cols) |col| {
                names.append(col.name) catch unreachable;
            }
            return names.toOwnedSlice() catch unreachable;
        }
        return null;
    }
};

pub const Options = union(enum) {
    Source: SourceOptions,
    Sink: SinkOptions,
};
