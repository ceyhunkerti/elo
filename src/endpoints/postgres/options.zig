const std = @import("std");
const c = @import("c.zig").c;

pub const ConnectionOptions = struct {
    host: [:0]const u8,
    database: [:0]const u8,
    username: [:0]const u8,
    password: [:0]const u8,
};

pub const SourceOptions = struct {
    connection: ConnectionOptions,
    fetch_size: u32 = 10_000,
    sql: [:0]const u8,
};

pub const SinkOptions = struct {
    allocator: std.mem.Allocator = undefined,
    connection: ConnectionOptions,
    table: [:0]const u8,
    columns: ?[][][:0]const u8 = null,
    sql: ?[:0]const u8 = null,
    mode: enum { Append, Truncate } = .Append,

    pub fn getCopySql(self: SinkOptions) ![:0]const u8 {
        if (self.sql) |sql| {
            return sql;
        }
        if (self.columns) |columns| {
            const columns_str = try std.mem.join(self.allocator, ",", columns);
            defer self.allocator.free(columns_str);
            const sql = try std.fmt.allocPrintZ(self.allocator, "copy {s} ({s}) from stdin", .{ self.table, columns_str });
            return sql;
        } else {
            return try std.fmt.allocPrintZ(self.allocator, "copy {s} from stdin", .{self.table});
        }
    }
};
