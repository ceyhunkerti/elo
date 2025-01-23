const std = @import("std");
const Connection = @import("../Connection.zig");
const SourceOptions = @import("../options.zig").SourceOptions;

const utils = @import("../utils.zig");
const w = @import("../../../wire/wire.zig");
const M = @import("../../../wire/M.zig");

const t = @import("../testing/testing.zig");

const Self = @This();

allocator: std.mem.Allocator,
conn: Connection = undefined,
options: SourceOptions,

pub fn init(allocator: std.mem.Allocator, options: SourceOptions) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = utils.initConnection(allocator, options.connection),
    };
}

pub fn deinit(self: *Self) !void {
    try self.conn.deinit();
}

pub fn connect(self: *Self) !void {
    try self.conn.connect();
}

pub fn run(self: *Self, wire: *w.Wire) !void {
    try self.read(wire);
}

pub fn read(self: *Self, wire: *w.Wire) !void {
    var stmt = try self.conn.prepareStatement(self.options.sql);
    defer stmt.deinit() catch unreachable;

    try stmt.execute();
    var rs = try stmt.getResultSet();
    defer rs.deinit();

    var it = rs.iterator();
    while (try it.next()) |record| {
        wire.put(try record.asMessage(self.allocator));
    }
    wire.put(w.Term(self.allocator));
}

test "Reader.read" {
    const allocator = std.testing.allocator;

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
    try reader.deinit();
}
