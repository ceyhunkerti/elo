const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;

const w = @import("../../../wire/wire.zig");
const c = @import("../c.zig").c;
const t = @import("../testing/testing.zig");

allocator: std.mem.Allocator,
conn: *Connection = undefined,
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
pub fn connect(self: Writer) !void {
    return try self.conn.connect();
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
