const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const Mailbox = @import("../../../wire/Mailbox.zig");
const Copy = @import("../Copy.zig");

const w = @import("../../../wire/wire.zig");
const c = @import("../c.zig").c;
const t = @import("../testing/testing.zig");
const p = @import("../../../wire/proto/proto.zig");

allocator: std.mem.Allocator,
conn: Connection = undefined,
options: SinkOptions,

pub fn init(allocator: std.mem.Allocator, options: SinkOptions) Writer {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = Connection.init(allocator, options.connection.username, options.connection.password, options.connection.host, options.connection.database),
    };
}
pub fn connect(self: *Writer) !void {
    return try self.conn.connect();
}

pub fn deinit(self: *Writer) void {
    self.conn.deinit();
}

pub fn getRecordFormatter(self: Writer) p.RecordFormatter {
    const options = self.options;
    var record_formatter = p.RecordFormatter{};
    if (options.copy_options) |co| {
        if (co.delimiter) |delimiter| {
            record_formatter.delimiters.field_delimiter = if (delimiter[0] == '\'' and delimiter[delimiter.len - 1] == '\'')
                delimiter[1 .. delimiter.len - 1]
            else
                delimiter;
        }
    }
    return record_formatter;
}

pub const WriteOptions = struct {
    record_formatter: p.RecordFormatter,
    copy: Copy,

    pub fn deinit(self: WriteOptions, allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }
};

pub fn run(self: *Writer, wire: *w.Wire) !void {
    const record_format = self.getRecordFormatter();
    const write_options = WriteOptions{
        .record_formatter = record_format,
        .copy = Copy.init(
            self.allocator,
            self.options.table,
            self.options.columns,
            self.options.copy_options,
        ),
    };
    defer write_options.deinit(self.allocator);

    try self.connect();
    try self.write(wire, write_options);
}

pub fn write(self: *Writer, wire: *w.Wire, options: WriteOptions) !void {
    var mb = try Mailbox.init(self.allocator, self.options.batch_size);
    defer mb.deinit();

    const copy_command = try options.copy.toString();
    defer self.allocator.free(copy_command);
    std.debug.print("COPY: \n{s}\n", .{copy_command});

    const res = c.PQexec(self.conn.pg_conn, @ptrCast(copy_command.ptr));
    if (c.PQresultStatus(res) != c.PGRES_COPY_IN) {
        std.debug.print("Error executing COPY: {s}\n", .{std.mem.span(c.PQresultErrorMessage(res))});
        return error.TransactionError;
    }
    c.PQclear(res);

    while (true) {
        const message = wire.get();
        switch (message.data) {
            .Metadata => mb.sendToMetadata(message),
            .Record => {
                mb.sendToInbox(message);
                if (mb.isInboxFull()) {
                    try self.writeBatch(&mb, options);
                    mb.clearInbox();
                }
            },
            .Nil => {
                mb.sendToNil(message);
                break;
            },
        }
    }
    if (mb.inboxNotEmpty()) {
        try self.writeBatch(&mb, options);
        mb.clearInbox();
    }

    if (c.PQputCopyEnd(self.conn.pg_conn, null) != 1) {
        std.debug.print("Failed to send COPY end signal\n", .{});
        return error.TransactionError;
    }
}

fn writeBatch(self: Writer, mb: *Mailbox, options: WriteOptions) !void {
    const data = try mb.inboxToString(self.allocator, options.record_formatter);
    defer self.allocator.free(data);

    std.debug.print("DATA: \n{s}\n", .{data});
    if (c.PQputCopyData(self.conn.pg_conn, @ptrCast(data.ptr), @intCast(data.len)) != 1) {
        std.debug.print("Failed to send COPY data: {s}\n", .{self.conn.errorMessage()});
        return error.TransactionError;
    }
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

    var wire = w.Wire.init();

    // first record
    const r1 = p.Record.fromSlice(
        allocator,
        &[_]p.Value{
            .{ .Int = 1 }, //id
            .{ .String = try allocator.dupe(u8, "John") }, //name
        },
    ) catch unreachable;
    const m1 = r1.asMessage(allocator) catch unreachable;
    wire.put(m1);

    // second record
    const r2 = p.Record.fromSlice(allocator, &[_]p.Value{
        .{ .Int = 2 }, //id
        .{ .String = try allocator.dupe(u8, "Jane") }, //name
    }) catch unreachable;
    const m2 = r2.asMessage(allocator) catch unreachable;
    wire.put(m2);

    // // third record with unicode
    // const record3 = p.Record.fromSlice(allocator, &[_]p.Value{
    //     .{ .Int = 3 }, //id
    //     .{ .String = try allocator.dupe(u8, "Έ Ή") }, //name
    // }) catch unreachable;
    // const m3 = record3.asMessage(allocator) catch unreachable;
    // wire.put(m3);

    wire.put(w.Term(allocator));

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
