const Reader = @This();

const shared = @import("../../shared.zig");
const std = @import("std");
const Connection = @import("../Connection.zig");
const SourceOptions = @import("../options.zig").SourceOptions;

const c = @import("../c.zig").c;
const p = @import("../../../wire/proto.zig");
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
        .sql = "select 1 as A, 2 as B, to_date('Monday, 27th January 2025', 'Day, DDth Month YYYY') as C",
    };

    var reader = Reader.init(allocator, options);
    defer reader.deinit();

    try reader.connect();
    var wire = w.Wire.init();
    try reader.run(&wire);

    const message = wire.get();
    defer M.deinit(allocator, message);

    const record = message.data.Record;

    try std.testing.expectEqual(record.len(), 3);
    try std.testing.expectEqual(record.get(0).Int, 1);
    try std.testing.expectEqual(record.get(1).Int, 2);

    const date = record.get(2).TimeStamp.?;
    try std.testing.expectEqual(date.year, 2025);
    try std.testing.expectEqual(date.month, 1);
    try std.testing.expectEqual(date.day, 27);
}
