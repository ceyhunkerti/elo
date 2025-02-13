const std = @import("std");
const Source = @import("Source.zig");
const Sink = @import("Sink.zig");
const base = @import("base");
const EndpointRegistry = base.EndpointRegistry;
const constants = @import("constants.zig");

pub fn source(allocator: std.mem.Allocator) !base.Source {
    const s = try allocator.create(Source);
    s.* = Source.init(allocator);
    return s.get();
}

pub fn sink(allocator: std.mem.Allocator) !base.Sink {
    const s = try allocator.create(Sink);
    s.* = Sink.init(allocator);
    return s.get();
}

pub fn register(registry: *EndpointRegistry) !void {
    try registry.sources.put(constants.NAME, try source(registry.allocator));
    try registry.sinks.put(constants.NAME, try sink(registry.allocator));
}

test {
    std.testing.refAllDecls(@This());
}

// test "oracle to oracle" {
//     const allocator = std.testing.allocator;
//     const tp = try t.getTestConnectionParams();
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

//     const target_table = try std.fmt.allocPrint(allocator, "{s}.ORACLE_TO_ORACLE_TEST", .{t.schema()});
//     defer allocator.free(target_table);

//     const to = o.SinkOptions{
//         .connection = co,
//         .table = target_table,
//         .mode = .Truncate,
//         .batch_size = 200,
//     };
//     var writer = Writer.init(allocator, to);
//     try writer.connect();

//     const create_sql = try std.fmt.allocPrint(
//         allocator,
//         \\CREATE TABLE {s} (
//         \\  ID NUMBER NOT NULL,
//         \\  NAME VARCHAR2(100)
//         \\)
//     ,
//         .{target_table},
//     );
//     defer allocator.free(create_sql);
//     try u.dropTableIfExists(writer.conn, target_table);
//     try u.executeCreateTable(writer.conn, create_sql);

//     const producer = struct {
//         pub fn producerThread(reader_: *Reader, wire: *w.Wire) !void {
//             reader_.run(wire) catch unreachable;
//         }
//     };
//     var wire = w.Wire.init();
//     var pth = try std.Thread.spawn(.{ .allocator = allocator }, producer.producerThread, .{ &reader, &wire });

//     writer.run(&wire) catch unreachable;

//     pth.join();

//     const count = try u.count(writer.conn, target_table);

//     try std.testing.expectEqual(count, @as(f64, @floatFromInt(duals.items.len)));

//     try u.dropTableIfExists(writer.conn, target_table);

//     try reader.deinit();
//     try writer.deinit();
// }
