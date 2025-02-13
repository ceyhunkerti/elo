const Reader = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const Cursor = @import("../Cursor.zig");
const SourceOptions = @import("../options.zig").SourceOptions;

const c = @import("../c.zig").c;
const base = @import("base");
const Wire = base.Wire;
const MessageFactory = base.MessageFactory;
const Record = base.Record;
const Value = base.Value;
const Term = base.Term;

const t = @import("../testing/testing.zig");

allocator: std.mem.Allocator,
conn: Connection,
options: SourceOptions,

pub fn init(allocator: std.mem.Allocator, options: SourceOptions) Reader {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = Connection.init(
            allocator,
            options.connection.username,
            options.connection.password,
            options.connection.host,
            options.connection.database,
        ),
    };
}
pub fn deinit(self: *Reader) void {
    self.conn.deinit();
}

pub fn info(allocator: std.mem.Allocator) ![]const u8 {
    return try allocator.dupe(u8, "hello from reader");
}

pub fn connect(self: *Reader) !void {
    try self.conn.connect();
}

pub fn run(self: *Reader, wire: *Wire) !void {
    if (!self.conn.isConnected()) {
        try self.connect();
    }

    const cursor_name = "elo_cursor";

    try self.conn.beginTransaction();
    defer self.conn.endTransaction() catch unreachable;

    var cursor = try self.conn.createCursor(cursor_name, self.options.sql);
    defer {
        cursor.close() catch unreachable;
        cursor.deinit();
    }

    try self.read(wire, &cursor);
    try wire.put(Term(self.allocator));
}

fn read(self: *Reader, wire: *Wire, cursor: *Cursor) !void {
    while (true) {
        const row_count = try cursor.execute();
        if (row_count == 0) break;
        while (try cursor.fetchNext()) |record| {
            try wire.put(try record.asMessage(self.allocator));
        }
    }
}
test "Reader.run" {
    const allocator = std.testing.allocator;

    const tp = t.connectionParams(allocator);
    const options = SourceOptions{
        .connection = .{
            .username = tp.username,
            .password = tp.password,
            .database = tp.database,
            .host = tp.host,
        },
        .fetch_size = 1,
        .sql =
        \\select 1 as A, 2 as B,
        \\to_date('Monday, 27th January 2025', 'Day, DDth Month YYYY') as C,
        \\to_timestamp('2025-01-27 02:03:44', 'YYYY-MM-DD HH24:MI:SS') as D,
        \\to_timestamp('2024-01-27 15:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF6') as E
        ,
    };

    var reader = Reader.init(allocator, options);
    defer reader.deinit();

    try reader.connect();
    var wire = Wire.init();
    try reader.run(&wire);

    const message = try wire.get();
    defer MessageFactory.destroy(allocator, message);

    const record = message.data.Record;

    try std.testing.expectEqual(record.len(), 5);
    try std.testing.expectEqual(record.get(0).Int, 1);
    try std.testing.expectEqual(record.get(1).Int, 2);

    const date = record.get(2).TimeStamp.?;
    try std.testing.expectEqual(date.year, 2025);
    try std.testing.expectEqual(date.month, 1);
    try std.testing.expectEqual(date.day, 27);

    const ts1 = record.get(3).TimeStamp.?;
    try std.testing.expectEqual(ts1.year, 2025);
    try std.testing.expectEqual(ts1.month, 1);
    try std.testing.expectEqual(ts1.day, 27);
    try std.testing.expectEqual(ts1.hour, 2);
    try std.testing.expectEqual(ts1.minute, 3);
    try std.testing.expectEqual(ts1.second, 44);

    const ts2 = record.get(4).TimeStamp.?;
    try std.testing.expectEqual(ts2.year, 2024);
    try std.testing.expectEqual(ts2.month, 1);
    try std.testing.expectEqual(ts2.day, 27);
    try std.testing.expectEqual(ts2.hour, 15);
    try std.testing.expectEqual(ts2.minute, 30);
    try std.testing.expectEqual(ts2.second, 45);
    try std.testing.expectEqual(ts2.nanosecond, 123456000);

    const term = try wire.get();
    defer MessageFactory.destroy(allocator, term);
    try std.testing.expectEqual(term.data, .Nil);
}
