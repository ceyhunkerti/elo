pub const Connection = @import("Connection.zig");
pub const Statement = @import("Statement.zig");
// pub const Reader = @import("io/Reader.zig");
// pub const Writer = @import("io/Writer.zig");

const std = @import("std");
test {
    std.testing.refAllDecls(@This());
}

const t = @import("testing/testing.zig");
const o = @import("./options.zig");
const w = @import("../../wire/wire.zig");
const p = @import("../../wire/proto/proto.zig");
const u = @import("utils.zig");

// test "oracle to oracle" {
//     const allocator = std.testing.allocator;
//     const target_table_name = "TEST_ORACLE_TO_ORACLE";

//     const tp = try t.connectionParams(allocator);
//     const co = .{
//         .connection_string = tp.connection_string,
//         .username = tp.username,
//         .password = tp.password,
//         .privilege = tp.privilege,
//     };

//     var duals = std.ArrayList([]const u8).init(allocator);
//     for (0..1_000) |i| {
//         const sql = try std.fmt.allocPrint(
//             allocator,
//             \\select
//             \\  cast({d} as number) as ID, cast('NAME_{d}' as varchar2(100)) as NAME
//             \\from dual
//         ,
//             .{ i, i },
//         );
//         duals.append(sql) catch unreachable;
//     }
//     defer {
//         for (duals.items) |sql| {
//             allocator.free(sql);
//         }
//         duals.deinit();
//     }
//     const source_sql = try std.mem.join(allocator, "\nunion all\n", duals.items);
//     defer allocator.free(source_sql);
//     // std.debug.print("source_sql: \n{s}\n", .{source_sql});

//     const so = o.SourceOptions{
//         .connection = co,
//         .fetch_size = 100,
//         .sql = source_sql,
//     };

//     var reader = Reader.init(allocator, so);
//     try reader.connect();
//     defer reader.deinit() catch unreachable;

//     const to = o.SinkOptions{
//         .connection = co,
//         .table = target_table_name,
//         .mode = .Truncate,
//         .batch_size = 200,
//     };
//     var writer = Writer.init(allocator, to);
//     try writer.connect();

//     const tt = t.TestTable.init(
//         allocator,
//         writer.conn,
//         target_table_name,
//         \\CREATE TABLE {name} (
//         \\  ID NUMBER NOT NULL,
//         \\  NAME VARCHAR2(100)
//         \\)
//         ,
//     );
//     try tt.createIfNotExists();
//     defer {
//         tt.dropIfExists() catch unreachable;
//         tt.deinit();
//         writer.deinit() catch unreachable;
//     }

//     const producer = struct {
//         pub fn producerThread(reader_: *Reader, wire: *w.Wire) !void {
//             reader_.run(wire) catch unreachable;
//         }
//     };
//     var wire = w.Wire.init();
//     var pth = try std.Thread.spawn(.{ .allocator = allocator }, producer.producerThread, .{ &reader, &wire });

//     writer.run(&wire) catch unreachable;

//     pth.join();

//     const count = try u.count(writer.conn, target_table_name);

//     try std.testing.expectEqual(count, @as(f64, @floatFromInt(duals.items.len)));
// }
