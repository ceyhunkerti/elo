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
    const table_name = "TEST_PG_COPY_01";
    const create_script = "CREATE TABLE TEST_PG_COPY_01 (ID INT not null, NAME VARCHAR(50) not null)";

    var tt = try t.TestTable(allocator, &conn, table_name, create_script);
    defer tt.deinit();
}
