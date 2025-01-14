const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const MessageQueue = @import("../../../queue.zig").MessageQueue;

const utils = @import("../utils.zig");

const Self = @This();

allocator: std.mem.Allocator,
conn: *Connection = undefined,
options: SinkOptions,

pub fn init(allocator: std.mem.Allocator, options: SinkOptions) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = utils.initConnection(allocator, options.connection),
    };
}

pub fn deinit(self: Self) !void {
    try self.conn.deinit();
    self.allocator.destroy(self.conn);
}

pub fn connect(self: Self) !void {
    return try self.conn.connect();
}

// pub fn buildInsertQuery(self: Self) ![]const u8 {
//     if (self.options.sql) |sql| {
//         return sql;
//     }
//     const table_name = self.options.table;
//     const sql = try std.fmt.allocPrint(self.allocator, "select * from {s} where 1=0", .{table_name});
//     defer self.allocator.free(sql);

//     var stmt = try self.conn.prepareStatement(sql);
//     try stmt.execute();
//     const qmd = try metadata.Query.init(self.allocator, &stmt);

//     const bindings = self.allocator.alloc(u8, qmd.columnCount() * 2 - 1) catch unreachable;
//     for (bindings, 0..) |*b, i| {
//         b.* = if (i % 2 == 0) '?' else ',';
//     }
//     return try std.fmt.allocPrint(self.allocator, "insert into {s} ({s}) values ({s})", .{
//         table_name,
//         try std.mem.join(self.allocator, ",", try qmd.columnNames()),
//         bindings,
//     });
// }

// pub fn buildInsertStatement(self: Oracle) !Statement {
//     const sql = self.buildInsertQuery();
//     return try self.conn.prepareStatement(sql);
// }

// pub fn write(self: Self, q: *MessageQueue) !void {}
