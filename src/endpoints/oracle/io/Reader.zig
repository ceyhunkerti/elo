const std = @import("std");
const Connection = @import("../Connection.zig");
const SourceOptions = @import("../options.zig").SourceOptions;
const MessageQueue = @import("../../../queue.zig").MessageQueue;

const t = @import("../testing/testing.zig");

const utils = @import("../utils.zig");
const Self = @This();

allocator: std.mem.Allocator,
conn: *Connection = undefined,
options: SourceOptions,

pub fn init(allocator: std.mem.Allocator, options: SourceOptions) Self {
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

pub fn read(self: Self, q: *MessageQueue) !void {
    var stmt = try self.conn.prepareStatement(self.options.sql);
    const column_count = try stmt.execute();
    while (true) {
        const record = try stmt.fetch(column_count) orelse break;
        const node = self.allocator.create(MessageQueue.Node) catch unreachable;
        node.* = .{ .data = .{ .Record = record } };
        q.put(node);
    }

    const term = self.allocator.create(MessageQueue.Node) catch unreachable;
    term.* = .{ .data = .Nil };
    q.put(term);
}

test read {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const tp = try t.getTestConnectionParams();
    const options = SourceOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .fetch_size = 1,
        .sql = "select 1 as A, 2 as B from dual",
    };

    var reader = Self.init(allocator, options);
    try reader.connect();
    var queue = MessageQueue.init();
    try reader.read(&queue);

    var message_count: usize = 0;
    var term_received: bool = false;
    var loop_count: usize = 0;
    while (true) : (loop_count += 1) {
        if (loop_count > 1) {
            unreachable;
        }
        switch (queue.get().data) {
            .Metadata => {},
            .Record => |record| {
                message_count += 1;
                try std.testing.expectEqual(record.len, 2);
                try std.testing.expectEqual(record[0].Double, 1);
                try std.testing.expectEqual(record[1].Double, 2);
            },
            .Nil => {
                term_received = true;
                break;
            },
        }
    }
}
