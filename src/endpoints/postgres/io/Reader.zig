const Reader = @This();

const shared = @import("../../shared.zig");
const std = @import("std");
const Connection = @import("../Connection.zig");
const SourceOptions = @import("../options.zig").SourceOptions;

const c = @import("../c.zig").c;
const p = @import("../../../wire/proto/proto.zig");
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
            options.connection.host,
            options.connection.database,
        ),
    };
}
pub fn deinit(self: *Reader) void {
    self.conn.deinit();
}

pub fn connect(self: *Reader) !void {
    try self.conn.connect();
}

pub fn run(self: *Reader, wire: *w.Wire) !void {
    try self.read(wire);
}

pub fn read(self: *Reader, wire: *w.Wire) !void {
    const cursor_name = "elo_cursor";

    var stmt = try self.conn.createStatement(self.options.sql);
    try stmt.createCursor(cursor_name);
    var md = try stmt.createCursorMetadata(cursor_name);

    defer {
        stmt.closeCursor(cursor_name) catch unreachable;
        stmt.conn.commit() catch unreachable;
        stmt.deinit();
        md.deinit();
    }

    const sql = try std.fmt.allocPrintZ(
        self.allocator,
        "FETCH {d} FROM {s}",
        .{ self.options.fetch_size, cursor_name },
    );
    defer self.allocator.free(sql);

    while (true) {
        const res = c.PQexec(self.conn.pg_conn, sql);
        defer c.PQclear(res);
        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
            // todo check error
            break;
        }
        const row_count: usize = @intCast(c.PQntuples(res));
        if (row_count == 0) break;

        for (0..row_count) |ri| {
            var record = try p.Record.init(self.allocator, md.columns.len);
            for (md.columns) |column| {
                const is_null = c.PQgetisnull(res, @intCast(ri), column.index);
                const str = if (is_null != 1) std.mem.span(c.PQgetvalue(res, @intCast(ri), @intCast(column.index))) else null;
                const val: p.Value = column.type.stringToValue(self.allocator, str);
                try record.append(val);
            }
            wire.put(try record.asMessage(self.allocator));
        }
    }
}
test "Reader.read" {
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
    // std.debug.print("SQL: {s}\n", .{options.sql});

    var reader = Reader.init(allocator, options);
    defer reader.deinit();

    try reader.connect();
    var wire = w.Wire.init();
    try reader.run(&wire);

    const message = wire.get();
    defer M.deinit(allocator, message);

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
}
