const Reader = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const SourceOptions = @import("../options.zig").SourceOptions;

const w = @import("../../../wire/wire.zig");
const M = @import("../../../wire/M.zig");
const t = @import("../testing/testing.zig");

allocator: std.mem.Allocator,
conn: Connection = undefined,
options: SourceOptions,

pub fn init(allocator: std.mem.Allocator, options: SourceOptions) Reader {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = Connection.init(
            allocator,
            options.connection.username,
            options.connection.password,
            options.connection.connection_string,
            options.connection.privilege,
        ),
    };
}

pub fn deinit(self: *Reader) void {
    self.conn.deinit();
}

pub fn connect(self: *Reader) !void {
    return try self.conn.connect();
}

pub fn run(self: *Reader, wire: *w.Wire) !void {
    try self.read(wire);
}

pub fn read(self: *Reader, wire: *w.Wire) !void {
    var stmt = try self.conn.prepareStatement(self.options.sql);
    const column_count = try stmt.execute();
    while (true) {
        const record = try stmt.fetch(column_count) orelse break;
        wire.put(try record.asMessage(self.allocator));
    }
    wire.put(w.Term(self.allocator));
}

test "Reader.read" {
    const allocator = std.testing.allocator;

    const tp = t.connectionParams(allocator);
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

    var reader = Reader.init(allocator, options);
    defer reader.deinit();
    try reader.connect();
    var wire = w.Wire.init();
    try reader.read(&wire);

    var message_count: usize = 0;
    var term_received: bool = false;
    var loop_count: usize = 0;
    while (true) : (loop_count += 1) {
        if (loop_count > 1) {
            unreachable;
        }
        const message = wire.get();
        defer M.deinit(allocator, message);

        switch (message.data) {
            .Metadata => {},
            .Record => |record| {
                message_count += 1;
                try std.testing.expectEqual(record.len(), 2);
                try std.testing.expectEqual(record.get(0).Double, 1);
                try std.testing.expectEqual(record.get(1).Double, 2);
            },
            .Nil => {
                term_received = true;
                break;
            },
        }
    }
}
