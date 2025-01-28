const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;

const w = @import("../../../wire/wire.zig");
const c = @import("../c.zig").c;

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

// pub fn write(self: *Writer, wire: *w.Wire) !void {

//     // if (PQputCopyData(conn, data[i], strlen(data[i])) != 1) {
//     //         fprintf(stderr, "Failed to send COPY data: %s", PQerrorMessage(conn));
//     //         exit_nicely(conn);
//     //     }

//     // const char *copyQuery = "COPY mytable (column1, column2, column3) FROM STDIN";
//     const sql = try self.options.getCopySql();

//     while (true) {
//         const message = wire.get();
//         switch (message.data) {
//             .Metadata => {},
//             .Record => |record| {},
//             .Nil => break,
//         }
//     }
// }
