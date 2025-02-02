const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const Statement = @import("../Statement.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const BindVariables = @import("../BindVariables.zig");

const utils = @import("../utils.zig");
const md = @import("../metadata/metadata.zig");
const c = @import("../c.zig").c;
const w = @import("../../../wire/wire.zig");
const p = @import("../../../wire/proto/proto.zig");
const t = @import("../testing/testing.zig");

allocator: std.mem.Allocator,
conn: *Connection = undefined,
options: SinkOptions,
batch_index: u32 = 0,
table: md.Table = undefined,

stmt: Statement = undefined,

pub fn init(allocator: std.mem.Allocator, options: SinkOptions) Writer {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = utils.initConnection(allocator, options.connection),
    };
}

pub fn deinit(self: Writer) !void {
    try self.conn.deinit();
    self.allocator.destroy(self.conn);
    self.table.deinit();
}

pub fn connect(self: Writer) !void {
    return try self.conn.connect();
}

test "Writer" {
    const tp = try t.connectionParams(std.testing.allocator);
    const options = SinkOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .table = "TEST_TABLE",
        .mode = .Truncate,
    };
    var writer = Writer.init(std.testing.allocator, options);
    try writer.connect();
    try writer.deinit();
}

pub fn prepare(self: *Writer) !void {
    // prepare table
    switch (self.options.mode) {
        .Append => return,
        .Truncate => {
            const sql = try std.fmt.allocPrint(self.allocator, "truncate table {s}", .{self.options.table});
            defer self.allocator.free(sql);
            _ = try self.conn.execute(sql);
        },
    }

    // get table metadata
    self.table = try md.getTableMetadata(self.allocator, self.conn, self.options.table);

    // prepare statement
    if (self.options.sql) |sql| {
        self.stmt = try self.conn.prepareStatement(sql);
    } else {
        const sql = try self.getInsertQuery();
        defer self.allocator.free(sql);
        self.stmt = try self.conn.prepareStatement(sql);
    }
}
test "Writer.[prepare, resetDpiVariables]" {
    const allocator = std.testing.allocator;
    const table_name = "TEST_WRITER_01";

    const tp = try t.connectionParams(allocator);
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

    const tt = t.TestTable.init(allocator, writer.conn, table_name, null);
    try tt.createIfNotExists();
    defer {
        tt.dropIfExists() catch unreachable;
        tt.deinit();
        writer.deinit() catch unreachable;
    }

    try writer.prepare();
}

test "Writer.write .Append" {}

pub fn write(self: *Writer, wire: *w.Wire) !void {
    var bv = try BindVariables.init(self.allocator, &self.stmt, self.table.columns, self.options.batch_size);
    defer bv.deinit() catch unreachable;

    var record_index: u32 = 0;

    while (true) {
        const message = wire.get();
        switch (message.data) {
            .Metadata => |m| m.deinit(self.allocator),
            .Record => |*record| {
                defer record.deinit(self.allocator);
                try bv.add(record_index, record);
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
    const tp = try t.connectionParams(allocator);

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
        writer.conn,
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
        writer.deinit() catch unreachable;
    }
    try tt.createIfNotExists();

    try writer.prepare();
    try std.testing.expectEqual(5, writer.table.columnCount());

    var wire = w.Wire.init();

    // first record
    const r1 = p.Record.fromSlice(
        allocator,
        &[_]p.Value{
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
    const r2 = p.Record.fromSlice(allocator, &[_]p.Value{
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
    const record3 = p.Record.fromSlice(allocator, &[_]p.Value{
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

    wire.put(w.Term(allocator));

    try writer.write(&wire);
    errdefer {
        std.debug.print("Error in writer.write with error: {s}\n", .{writer.conn.errorMessage()});
    }

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

pub fn run(self: *Writer, wire: *w.Wire) !void {
    try self.prepare();
    try self.write(wire);
}

fn getInsertQuery(self: Writer) ![]const u8 {
    const allocator = self.table.allocator;
    const column_names = try self.table.columnNames();
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
    , .{ self.table.name.name, cols, ci.bindings });
    return sql;
}

test "batch-insert" {
    const allocator = std.testing.allocator;
    const table_name = "TEST_BATCH_INSERT";

    const tp = try t.connectionParams(allocator);
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

    const create_script =
        \\CREATE TABLE {name} (
        \\    id NUMBER,
        \\    name VARCHAR2(10)
        \\)
    ;
    const tt = t.TestTable.init(allocator, writer.conn, table_name, create_script);
    try tt.createIfNotExists();

    defer {
        tt.dropIfExists() catch unreachable;
        tt.deinit();
        writer.deinit() catch unreachable;
    }

    var dpi_var_array: ?[]?*c.dpiVar = null;
    var dpi_data_array: ?[]?[*c]c.dpiData = null;

    dpi_var_array = try allocator.alloc(?*c.dpiVar, 2);
    dpi_data_array = try allocator.alloc(?[*c]c.dpiData, 2);
    defer if (dpi_var_array) |a| allocator.free(a);
    defer if (dpi_data_array) |a| allocator.free(a);

    if (c.dpiConn_newVar(
        writer.conn.dpi_conn,
        c.DPI_ORACLE_TYPE_NUMBER,
        c.DPI_NATIVE_TYPE_DOUBLE,
        1,
        0,
        0,
        0,
        null,
        &dpi_var_array.?[0],
        &dpi_data_array.?[0].?,
    ) < 0) {
        unreachable;
    }

    if (c.dpiConn_newVar(
        writer.conn.dpi_conn,
        c.DPI_ORACLE_TYPE_VARCHAR,
        c.DPI_NATIVE_TYPE_BYTES,
        1,
        10,
        0,
        0,
        null,
        &dpi_var_array.?[1],
        &dpi_data_array.?[1].?,
    ) < 0) {
        unreachable;
    }
    const slq = try std.fmt.allocPrint(allocator, "insert into {s} (id, name) values (:1, :2)", .{table_name});
    defer allocator.free(slq);

    writer.stmt = writer.conn.prepareStatement(slq) catch unreachable;

    if (c.dpiStmt_bindByPos(writer.stmt.dpi_stmt, 1, dpi_var_array.?[0]) < 0) {
        std.debug.print("c.dpiStmt_bindByPos error: {s}\n", .{writer.conn.errorMessage()});
        unreachable;
    }
    if (c.dpiStmt_bindByPos(writer.stmt.dpi_stmt, 2, dpi_var_array.?[1]) < 0) {
        unreachable;
    }

    // first row
    // set id
    dpi_data_array.?[0].?[0].isNull = 0;
    dpi_data_array.?[0].?[0].value.asDouble = 1;

    // Set name
    dpi_data_array.?[1].?[0].isNull = 0; // Add this line
    if (c.dpiVar_setFromBytes(dpi_var_array.?[1].?, 0, "s1".ptr, 2) < 0) { // Change index to 0
        std.debug.print("c.dpiVar_setFromBytes error: {s}\n", .{writer.conn.errorMessage()});
        unreachable;
    }

    try writer.stmt.executeMany(1);
    try writer.conn.commit();
}
