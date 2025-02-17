const Reader = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const Cursor = @import("../Cursor.zig");
const Allocator = std.mem.Allocator;
const constants = @import("../constants.zig");

const c = @import("../c.zig").c;
const base = @import("base");
const Wire = base.Wire;
const MessageFactory = base.MessageFactory;
const Record = base.Record;
const Value = base.Value;

const log = std.log;
const t = @import("../testing/testing.zig");

const FETCH_SIZE = constants.FETCH_SIZE;
const CURSOR_NAME = constants.CURSOR_NAME;

allocator: std.mem.Allocator,

// connection. Each reader should have a separate connection.
conn: *Connection,

// sql to be executed on the connection.
sql: []const u8,

// reader index. Used to identify reader in parallel sessions.
reader_index: u16,

fetch_size: u32 = FETCH_SIZE,

// cursor name is in the for of [CURSOR_NAME]_{reader_index}
cursor_name: []const u8,

pub fn init(allocator: Allocator, reader_index: u16, conn: *Connection, sql: []const u8, fetch_size: ?u32) Reader {
    return .{
        .allocator = allocator,
        .reader_index = reader_index,
        .conn = conn,
        .sql = sql,
        .fetch_size = fetch_size orelse FETCH_SIZE,
        .cursor_name = std.fmt.allocPrint(allocator, "{s}_{d}", .{ CURSOR_NAME, reader_index }) catch unreachable,
    };
}

pub fn deinit(self: *Reader) void {
    self.allocator.free(self.cursor_name);
    self.conn.deinit(self.allocator);
    self.allocator.destroy(self.conn);
}

pub fn run(self: *Reader, wire: *Wire) !void {
    log.debug("Starting Reader.run for reader {d} with SQL {s}", .{ self.reader_index, self.sql });
    wire.startProducer();
    defer wire.stopProducer();

    errdefer |err| {
        wire.interruptWithError(self.allocator, err);
    }

    try self.conn.beginTransaction();

    var cursor = try self.conn.createCursor(self.cursor_name, self.sql, self.fetch_size);
    try self.read(wire, &cursor);

    {
        try self.conn.endTransaction();
        try cursor.close();
        cursor.deinit();
    }
}

fn read(self: *Reader, wire: *Wire, cursor: *Cursor) !void {
    while (true) {
        const row_count = try cursor.execute();
        if (row_count == 0) break;
        while (try cursor.fetchNext()) |record| {
            const msg = try record.asMessage(self.allocator);
            wire.put(msg) catch |err| {
                MessageFactory.destroy(self.allocator, msg);
                return err;
            };
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
    var wire = Wire.init(1, 1);
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
