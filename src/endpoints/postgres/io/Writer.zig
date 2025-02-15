const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const Copy = @import("../Copy.zig");

const c = @import("../c.zig").c;
const t = @import("../testing/testing.zig");
const base = @import("base");
const Wire = base.Wire;
const MessageFactory = base.MessageFactory;
const RecordFormatter = base.RecordFormatter;
const Record = base.Record;
const Value = base.Value;
const Term = base.Term;

allocator: std.mem.Allocator,
conn: Connection = undefined,
options: SinkOptions,

pub fn init(allocator: std.mem.Allocator, options: SinkOptions) Writer {
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
pub fn deinit(self: *Writer) void {
    self.conn.deinit();
}

pub fn connect(self: *Writer) !void {
    return try self.conn.connect();
}

pub fn info(allocator: std.mem.Allocator) ![]const u8 {
    var output = std.ArrayList(u8).init(allocator);
    defer output.deinit();
    try SinkOptions.help(&output);
    return try output.toOwnedSlice();
}

fn getRecordFormatter(self: Writer) RecordFormatter {
    const options = self.options;
    var record_formatter = RecordFormatter{};
    if (options.copy_options) |co| {
        record_formatter.delimiters.field_delimiter = if (co.delimiter[0] == '\'' and co.delimiter[co.delimiter.len - 1] == '\'')
            co.delimiter[1 .. co.delimiter.len - 1]
        else
            co.delimiter;
    }
    return record_formatter;
}

pub fn run(self: *Writer, wire: *Wire) !void {
    errdefer |err| {
        wire.interruptWithError(self.allocator, err);
    }
    if (!self.conn.isConnected()) {
        try self.connect();
    }
    try self.write(wire);
}

pub fn write(self: *Writer, wire: *Wire) !void {
    const formatter = self.getRecordFormatter();
    var copy = Copy.init(
        self.allocator,
        &self.conn,
        self.options.table,
        self.options.columns,
        self.options.copy_options,
        self.options.batch_size,
    );
    try copy.start();

    while (true) {
        const message = try wire.get();
        defer MessageFactory.destroy(self.allocator, message);
        switch (message.data) {
            .Metadata => {},
            .Record => |*record| {
                try copy.copy(record, formatter);
            },
            .Nil => break,
        }
    }

    try copy.finish();
}

test "Writer.run" {
    const allocator = std.testing.allocator;
    const tp = t.connectionParams(allocator);

    var writer = Writer.init(allocator, .{
        .connection = .{
            .username = tp.username,
            .password = tp.password,
            .host = tp.host,
            .database = tp.database,
        },
        .table = "TEST_WRITER_RUN_01",
        .mode = .Truncate,
        .columns = &.{ "ID", "NAME" },
        .copy_options = .{
            .format = "CSV",
            .delimiter = "','",
        },
    });
    defer writer.deinit();
    try writer.connect();
    writer.conn.execute("CREATE TABLE IF NOT EXISTS TEST_WRITER_RUN_01 (ID INT not null, NAME VARCHAR(50) not null)") catch unreachable;

    var wire = Wire.init();

    // first record
    const r1 = Record.fromSlice(
        allocator,
        &[_]Value{
            .{ .Int = 1 }, //id
            .{ .Bytes = try allocator.dupe(u8, "John") }, //name
        },
    ) catch unreachable;
    const m1 = r1.asMessage(allocator) catch unreachable;
    try wire.put(m1);

    // second record
    const r2 = Record.fromSlice(allocator, &[_]Value{
        .{ .Int = 2 }, //id
        .{ .Bytes = try allocator.dupe(u8, "Jane") }, //name
    }) catch unreachable;
    const m2 = r2.asMessage(allocator) catch unreachable;
    try wire.put(m2);

    try wire.put(Term(allocator));

    try writer.run(&wire);
}

test "postgres.copy" {
    const allocator = std.testing.allocator;
    var conn = t.connection(allocator);
    try conn.connect();
    defer conn.deinit();
    // const table_name = "TEST_PG_COPY_01";

    const drop_script: [:0]const u8 = "DROP TABLE IF EXISTS TEST_PG_COPY_01";
    const create_script: [:0]const u8 = "CREATE TABLE IF NOT EXISTS TEST_PG_COPY_01 (ID INT not null, NAME VARCHAR(50) not null)";
    const copy_sql: [:0]const u8 = "COPY TEST_PG_COPY_01 (ID, NAME) FROM STDIN  WITH (FORMAT CSV, DELIMITER '|');";

    try conn.execute(drop_script);
    try conn.execute(create_script);

    const res = c.PQexec(conn.pg_conn, copy_sql);
    if (c.PQresultStatus(res) != c.PGRES_COPY_IN) {
        std.debug.print("Error executing COPY: {s}\n", .{conn.errorMessage()});
        return error.TransactionError;
    }
    c.PQclear(res);

    const data: [:0]const u8 = "1|John\n"; // Ensure data is properly formatted

    // Begin sending COPY data
    if (c.PQputCopyData(conn.pg_conn, data.ptr, @intCast(data.len)) != 1) {
        std.debug.print("Failed to send COPY data: {s}\n", .{conn.errorMessage()});
        conn.deinit();
        return error.TransactionError;
    }

    // End the COPY operation
    if (c.PQputCopyEnd(conn.pg_conn, null) != 1) {
        std.debug.print("Failed to send COPY end signal\n", .{});
        conn.deinit();
        return error.TransactionError;
    }

    // Fetch the result of the COPY operation
    const result = c.PQgetResult(conn.pg_conn);
    if (c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
        std.debug.print("COPY command failed: {s}\n", .{c.PQerrorMessage(conn.pg_conn)});
        c.PQclear(result);
        c.PQfinish(conn.pg_conn);
        return error.TransactionError;
    }
    c.PQclear(result);

    // Cleanup and close the connection
    c.PQfinish(conn.pg_conn);
}
