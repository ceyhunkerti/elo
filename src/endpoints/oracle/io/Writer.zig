const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const Statement = @import("../Statement.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const ArrayBind = @import("../ArrayBind.zig");

const base = @import("base");
const Wire = base.Wire;
const MessageFactory = base.MessageFactory;
const Record = base.Record;
const Value = base.Value;
const Term = base.Term;

const md = @import("../metadata/metadata.zig");
const c = @import("../c.zig").c;
const t = @import("../testing/testing.zig");

allocator: std.mem.Allocator,
conn: Connection,
options: SinkOptions,
batch_index: u32 = 0,

table: ?md.Table = null,

stmt: Statement = undefined,

pub fn init(allocator: std.mem.Allocator, options: SinkOptions) Writer {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = Connection.init(
            allocator,
            options.connection.username,
            options.connection.password,
            options.connection.connection_string,
            options.connection.privilege,
        ),
    };
}
pub fn initAndConnect(allocator: std.mem.Allocator, options: SinkOptions) !Writer {
    var writer = Writer.init(allocator, options);
    try writer.connect();
    return writer;
}

pub fn help(_: Writer) ![]const u8 {
    return "";
}

pub fn deinit(self: *Writer) void {
    self.conn.deinit();
    if (self.table) |*table| table.deinit();
}

pub fn connect(self: *Writer) !void {
    try self.conn.connect();
}

pub fn prepare(self: *Writer) !void {
    // prepare table
    switch (self.options.mode) {
        .Append => {},
        .Truncate => try self.conn.truncate(self.options.table),
    }

    // get table metadata
    self.table = try md.getTableMetadata(self.allocator, &self.conn, self.options.table);

    // prepare statement
    if (self.options.sql) |sql| {
        self.stmt = try self.conn.prepareStatement(sql);
    } else {
        const sql = try self.getInsertQuery();
        defer self.allocator.free(sql);
        self.stmt = try self.conn.prepareStatement(sql);
    }
}
test "Writer.prepare" {
    const allocator = std.testing.allocator;
    const table_name = "TEST_WRITER_01";

    const tp = t.connectionParams(allocator);
    const options = SinkOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .table = table_name,
        .mode = .Truncate,
    };
    var writer = Writer.init(allocator, options);
    try writer.connect();

    const tt = t.TestTable.init(allocator, &writer.conn, table_name, null);
    try tt.createIfNotExists();
    defer {
        tt.dropIfExists() catch unreachable;
        tt.deinit();
        writer.deinit();
    }

    try writer.prepare();
}

test "Writer.write .Append" {}

pub fn write(self: *Writer, wire: *Wire) !void {
    var ab = try ArrayBind.init(self.allocator, &self.stmt, self.table.?.columns, self.options.batch_size);
    defer ab.deinit();

    var record_index: u32 = 0;

    while (true) {
        const message = wire.get();
        defer MessageFactory.destroy(self.allocator, message);
        switch (message.data) {
            .Metadata => {},
            .Record => |*record| {
                try ab.add(record_index, record);
                record_index += 1;
                if (record_index == self.options.batch_size) {
                    try self.writeBatch(record_index);
                    record_index = 0;
                }
            },
            .Nil => break,
        }
    }

    if (record_index > 0) try self.writeBatch(record_index);

    try self.conn.commit();
}
test "Writer.write" {
    const allocator = std.testing.allocator;
    const table_name = "TEST_WRITER_02";
    const tp = t.connectionParams(allocator);

    const options = SinkOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .table = table_name,
        .mode = .Truncate,
        .batch_size = 2,
    };
    var writer = Writer.init(allocator, options);
    try writer.connect();

    const tt = t.TestTable.init(
        allocator,
        &writer.conn,
        table_name,
        \\CREATE TABLE {name} (
        \\    id NUMBER,
        \\    name VARCHAR2(10 char),
        \\    age NUMBER,
        \\    birth_date DATE,
        \\    is_active NUMBER
        \\)
        ,
    );
    defer {
        tt.dropIfExists() catch unreachable;
        tt.deinit();
        writer.deinit();
    }
    try tt.createIfNotExists();

    try writer.prepare();
    try std.testing.expectEqual(5, writer.table.?.columnCount());

    var wire = Wire.init();

    // first record
    const r1 = Record.fromSlice(
        allocator,
        &[_]Value{
            .{ .Int = 1 }, //id
            .{ .Bytes = try allocator.dupe(u8, "John") }, //name
            .{ .Int = 20 }, //age
            .{ .TimeStamp = .{
                .year = 2000,
                .month = 1,
                .day = 1,
                .hour = 0,
                .minute = 0,
                .second = 0,
                .nanosecond = 0,
                .tz_offset = .{ .hours = 0, .minutes = 0 },
            } }, //birth_date
            .{ .Boolean = true }, //is_active
        },
    ) catch unreachable;
    const m1 = r1.asMessage(allocator) catch unreachable;
    wire.put(m1);

    // second record
    const r2 = Record.fromSlice(allocator, &[_]Value{
        .{ .Int = 2 }, //id
        .{ .Bytes = try allocator.dupe(u8, "Jane") }, //name
        .{ .Int = 21 }, //age
        .{ .TimeStamp = .{
            .year = 2000,
            .month = 1,
            .day = 1,
            .hour = 0,
            .minute = 0,
            .second = 0,
            .nanosecond = 0,
            .tz_offset = .{ .hours = 0, .minutes = 0 },
        } }, //birth_date
        .{ .Boolean = false }, //is_active
    }) catch unreachable;
    const m2 = r2.asMessage(allocator) catch unreachable;
    wire.put(m2);

    // third record with unicode
    const record3 = Record.fromSlice(allocator, &[_]Value{
        .{ .Int = 3 }, //id
        .{ .Bytes = try allocator.dupe(u8, "Έ Ή") }, //name
        .{ .Int = 22 }, //age
        .{ .TimeStamp = .{
            .year = 2000,
            .month = 1,
            .day = 1,
            .hour = 0,
            .minute = 0,
            .second = 0,
            .nanosecond = 0,
            .tz_offset = .{ .hours = 0, .minutes = 0 },
        } }, //birth_date
        .{ .Boolean = true }, //is_active
    }) catch unreachable;
    const m3 = record3.asMessage(allocator) catch unreachable;
    wire.put(m3);

    wire.put(Term(allocator));

    try writer.write(&wire);
    const check_query = try std.fmt.allocPrint(
        allocator,
        "SELECT id, name, age, birth_date, is_active FROM {s} order by id",
        .{table_name},
    );
    defer allocator.free(check_query);

    var stmt = writer.conn.prepareStatement(check_query) catch unreachable;
    const column_count = try stmt.execute();
    var record_count: usize = 0;
    while (true) {
        const row = stmt.fetch(column_count) catch {
            std.debug.print("Error in writer.write with error: {s}\n", .{writer.conn.errorMessage()});
            unreachable;
        };
        if (row == null) break;
        defer row.?.deinit(allocator);

        switch (record_count) {
            0 => {
                try std.testing.expectEqual(1, row.?.get(0).Double.?);
                try std.testing.expectEqualStrings("John", row.?.get(1).Bytes.?);
            },
            1 => {
                try std.testing.expectEqual(2, row.?.get(0).Double.?);
                try std.testing.expectEqualStrings("Jane", row.?.get(1).Bytes.?);
            },
            2 => {
                try std.testing.expectEqual(3, row.?.get(0).Double.?);
                try std.testing.expectEqualStrings("Έ Ή", row.?.get(1).Bytes.?);
            },
            else => unreachable,
        }
        record_count += 1;
    }

    try std.testing.expectEqual(3, record_count);
}

fn writeBatch(self: *Writer, size: u32) !void {
    self.stmt.executeMany(size) catch {
        std.debug.print("Failed to executeMany with error: {s}\n", .{self.conn.errorMessage()});
        unreachable;
    };
    self.batch_index += size;
}

pub fn run(self: *Writer, wire: *Wire) !void {
    if (!self.conn.isConnected()) {
        try self.connect();
    }
    try self.prepare();
    try self.write(wire);
}

fn getInsertQuery(self: Writer) ![]const u8 {
    const allocator = self.table.?.allocator;
    const column_names = try self.table.?.columnNames();
    var bindings = std.ArrayList(u8).init(allocator);

    const ColumnInfo = struct {
        column_names: []const []const u8,
        bindings: []const u8,

        pub fn deinit(this: @This(), alloc: std.mem.Allocator) void {
            alloc.free(this.column_names);
            alloc.free(this.bindings);
        }
    };

    const ci: ColumnInfo = brk: {
        if (self.options.columns == null) {
            for (0..column_names.len) |i| {
                try bindings.writer().print(":{d}", .{i});
                if (i < column_names.len - 1) try bindings.appendSlice(",");
            }
            break :brk .{
                .column_names = column_names,
                .bindings = try bindings.toOwnedSlice(),
            };
        } else {
            defer allocator.free(column_names);
            var i: u16 = 1;
            var filtered_column_names = std.ArrayList([]const u8).init(allocator);
            for (column_names) |cn| {
                for (self.options.columns.?) |bn| {
                    if (std.mem.eql(u8, cn, bn)) {
                        try bindings.writer().print(":{d}", .{i});
                        try filtered_column_names.append(cn);
                        if (i < column_names.len - 1) {
                            try bindings.appendSlice(",");
                        }
                        i += 1;
                    }
                }
            }
            break :brk .{
                .column_names = try filtered_column_names.toOwnedSlice(),
                .bindings = try bindings.toOwnedSlice(),
            };
        }
    };
    defer ci.deinit(allocator);

    const cols = try std.mem.join(allocator, ",", ci.column_names);
    defer allocator.free(cols);

    const sql = try std.fmt.allocPrint(allocator,
        \\INSERT INTO {s} ({s}) VALUES ({s})
    , .{ self.table.?.name.name, cols, ci.bindings });
    return sql;
}
