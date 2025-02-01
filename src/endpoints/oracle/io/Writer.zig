const Writer = @This();

const std = @import("std");
const Connection = @import("../Connection.zig");
const Statement = @import("../Statement.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const Mailbox = @import("../../../wire/Mailbox.zig");
const TableMetadata = @import("../metadata/TableMetadata.zig");

const utils = @import("../utils.zig");
const c = @import("../c.zig").c;
const w = @import("../../../wire/wire.zig");
const p = @import("../../../wire/proto/proto.zig");
const t = @import("../testing/testing.zig");

allocator: std.mem.Allocator,
conn: *Connection = undefined,
options: SinkOptions,
batch_index: u32 = 0,

table_metadata: TableMetadata = .{},
dpi_variables: struct {
    dpi_var_array: ?[]?*c.dpiVar = null,
    dpi_data_array: ?[]?[*c]c.dpiData = null,
} = .{},

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
    self.table_metadata.deinit();

    try self.clearDpiVariables();
    if (self.dpi_variables.dpi_var_array) |arr| self.allocator.free(arr);
    if (self.dpi_variables.dpi_data_array) |arr| self.allocator.free(arr);
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

pub fn clearDpiVariables(self: Writer) !void {
    if (self.dpi_variables.dpi_var_array) |arr| for (arr) |var_| {
        if (var_) |v| {
            if (c.dpiVar_release(v) > 0) {
                std.debug.print("Failed to release variable with error: {s}\n", .{self.conn.errorMessage()});
                return error.Fail;
            }
        }
    };
}

pub fn initDpiVariables(self: *Writer) !void {
    self.dpi_variables.dpi_var_array = try self.allocator.alloc(?*c.dpiVar, self.table_metadata.columnCount());
    self.dpi_variables.dpi_data_array = try self.allocator.alloc(?[*c]c.dpiData, self.table_metadata.columnCount());

    for (self.table_metadata.columns.?, 0..) |column, ci| {
        try self.conn.newVariable(
            column.dpi_oracle_type_num,
            column.dpi_native_type_num,
            self.options.batch_size,
            column.dpiVarSize(),
            false, // todo size_is_bytes
            false, // todo is_array
            null,
            &self.dpi_variables.dpi_var_array.?[ci],
            &self.dpi_variables.dpi_data_array.?[ci].?,
        );
    }

    for (0..self.table_metadata.columnCount()) |i| {
        if (c.dpiStmt_bindByPos(
            self.stmt.dpi_stmt,
            @as(u32, @intCast(i)) + 1,
            self.dpi_variables.dpi_var_array.?[i],
        ) < 0) {
            unreachable;
        }
    }
}

pub fn prepare(self: *Writer) !void {
    // prepare table
    switch (self.options.mode) {
        .Append => return,
        .Truncate => try utils.truncateTable(self.conn, self.options.table),
    }

    self.table_metadata = try TableMetadata.fetch(
        self.allocator,
        self.conn,
        self.options.table,
    );

    if (self.options.sql) |sql| {
        self.stmt = try self.conn.prepareStatement(sql);
    } else {
        const sql = try self.table_metadata.insertQuery(self.options.columns);
        defer self.allocator.free(sql);
        self.stmt = try self.conn.prepareStatement(sql);
    }
    try self.initDpiVariables();
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
    var mb = try Mailbox.init(self.allocator, self.options.batch_size);
    defer mb.deinit();

    while (true) {
        const message = wire.get();
        switch (message.data) {
            .Metadata => mb.sendToMetadata(message),
            .Record => {
                mb.sendToInbox(message);
                if (mb.isInboxFull()) {
                    try self.writeBatch(&mb);
                    try self.conn.commit();
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
        try self.writeBatch(&mb);
        try self.conn.commit();
        mb.clearInbox();
    }

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
    try std.testing.expectEqual(5, writer.table_metadata.columnCount());

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

pub fn writeBatch(self: *Writer, mb: *Mailbox) !void {
    for (0..mb.inbox_index) |ri| {
        const record = mb.inbox[ri].*.data.Record;
        for (record.items(), 0..) |column, ci| {
            switch (column) {
                .Int => |val| {
                    if (val) |v| {
                        switch (self.table_metadata.columns.?[ci].dpi_native_type_num) {
                            c.DPI_NATIVE_TYPE_INT64 => {
                                self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asInt64 = v;
                            },
                            c.DPI_NATIVE_TYPE_DOUBLE => {
                                self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asDouble = @floatFromInt(v);
                            },
                            c.DPI_NATIVE_TYPE_FLOAT => {
                                self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asFloat = @floatFromInt(v);
                            },
                            c.DPI_NATIVE_TYPE_BYTES => {
                                const str = try std.fmt.allocPrint(self.allocator, "{d}", .{v});
                                defer self.allocator.free(str);
                                if (c.dpiVar_setFromBytes(self.dpi_variables.dpi_var_array.?[ci].?, @intCast(ri), str.ptr, @intCast(str.len)) < 0) {
                                    std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.conn.errorMessage()});
                                    unreachable;
                                }
                            },
                            else => {
                                std.debug.print(
                                    \\Type conversion from int to oracle native type num {d} is not supported
                                    \\Column name: {s}
                                    \\Column oracle type num: {d}
                                , .{
                                    self.table_metadata.columns.?[ci].dpi_native_type_num,
                                    self.table_metadata.columns.?[ci].name,
                                    self.table_metadata.columns.?[ci].dpi_oracle_type_num,
                                });
                                return error.Fail;
                            },
                        }
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                    } else {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .Double => |val| {
                    if (val) |v| {
                        switch (self.table_metadata.columns.?[ci].dpi_native_type_num) {
                            c.DPI_NATIVE_TYPE_INT64 => {
                                self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asInt64 = @intFromFloat(v);
                            },
                            c.DPI_NATIVE_TYPE_DOUBLE => {
                                self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asDouble = v;
                            },
                            c.DPI_NATIVE_TYPE_BYTES => {
                                const str = try std.fmt.allocPrint(self.allocator, "{d}", .{v});
                                defer self.allocator.free(str);
                                if (c.dpiVar_setFromBytes(self.dpi_variables.dpi_var_array.?[ci].?, @intCast(ri), str.ptr, @intCast(str.len)) < 0) {
                                    std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.conn.errorMessage()});
                                    unreachable;
                                }
                            },
                            else => {
                                std.debug.print(
                                    \\Type conversion from int to oracle native type num {d} is not supported
                                    \\Column name: {s}
                                    \\Column oracle type num: {d}
                                , .{
                                    self.table_metadata.columns.?[ci].dpi_native_type_num,
                                    self.table_metadata.columns.?[ci].name,
                                    self.table_metadata.columns.?[ci].dpi_oracle_type_num,
                                });
                                return error.Fail;
                            },
                        }
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                    } else {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .Bytes => |val| {
                    if (val) |v| {
                        if (self.table_metadata.columns.?[ci].dpi_native_type_num != c.DPI_NATIVE_TYPE_BYTES) {
                            std.debug.print(
                                \\Type conversion from string to oracle native type num {d} is not supported
                                \\Column name: {s}
                                \\Column oracle type num: {d}
                            , .{
                                self.table_metadata.columns.?[ci].dpi_native_type_num,
                                self.table_metadata.columns.?[ci].name,
                                self.table_metadata.columns.?[ci].dpi_oracle_type_num,
                            });
                            return error.Fail;
                        }
                        if (c.dpiVar_setFromBytes(self.dpi_variables.dpi_var_array.?[ci].?, @intCast(ri), v.ptr, @intCast(v.len)) < 0) {
                            std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.conn.errorMessage()});
                            unreachable;
                        }
                    } else {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .Boolean => |val| {
                    if (val) |v| {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asBoolean = if (v) 1 else 0;
                    } else {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .TimeStamp => |val| {
                    if (val) |v| {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asTimestamp.year = @intCast(v.year);
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asTimestamp.month = v.month;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asTimestamp.day = v.day;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asTimestamp.hour = v.hour;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asTimestamp.minute = v.minute;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asTimestamp.second = v.second;
                    } else {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                // todo
                else => {
                    std.debug.print("\nUnhandled column in writer {any}\n", .{column});
                    unreachable;
                },
            }
        }
    }

    self.stmt.executeMany(mb.inbox_index) catch {
        std.debug.print("Failed to executeMany with error: {s}\n", .{self.conn.errorMessage()});
        unreachable;
    };
}

pub fn run(self: *Writer, wire: *w.Wire) !void {
    try self.prepare();
    try self.write(wire);
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
