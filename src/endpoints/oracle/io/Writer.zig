const c = @import("../c.zig").c;
const std = @import("std");
const Connection = @import("../Connection.zig");
const Statement = @import("../Statement.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const queue = @import("../../../queue.zig");
const TableMetadata = @import("../metadata/TableMetadata.zig");
const CreateTableScript = @import("../metadata/script.zig").CreateTableScript;
const commons = @import("../../../commons.zig");
const Record = commons.Record;
const t = @import("../testing/testing.zig");
const utils = @import("../utils.zig");
const zdt = @import("zdt");

const Self = @This();

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

const Error = error{
    FailedToClearDpiVariables,
};

pub fn init(allocator: std.mem.Allocator, options: SinkOptions) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .conn = utils.initConnection(allocator, options.connection),
    };
}

pub fn deinit(self: Self) !void {
    try self.conn.deinit();
    self.allocator.destroy(self.conn);
    self.table_metadata.deinit();

    try self.clearDpiVariables();
    if (self.dpi_variables.dpi_var_array) |arr| self.allocator.free(arr);
    if (self.dpi_variables.dpi_data_array) |arr| self.allocator.free(arr);
}

pub fn connect(self: Self) !void {
    return try self.conn.connect();
}

test "Writer" {
    const tp = try t.getTestConnectionParams();
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
    var writer = Self.init(std.testing.allocator, options);
    try writer.connect();
    try writer.deinit();
}

pub fn clearDpiVariables(self: Self) !void {
    if (self.dpi_variables.dpi_var_array) |arr| for (arr) |var_| {
        if (var_) |v| {
            if (c.dpiVar_release(v) > 0) {
                std.debug.print("Failed to release variable with error: {s}\n", .{self.conn.errorMessage()});
                return error.FailedToClearDpiVariables;
            }
        }
    };
}

pub fn initDpiVariables(self: *Self) !void {
    self.dpi_variables.dpi_var_array = try self.allocator.alloc(?*c.dpiVar, self.table_metadata.columnCount());
    self.dpi_variables.dpi_data_array = try self.allocator.alloc(?[*c]c.dpiData, self.table_metadata.columnCount());

    for (self.table_metadata.columns.?, 0..) |column, ci| {
        std.debug.print("oracle {d} native {d}\n", .{ column.oracle_type_num, column.native_type_num });

        if (c.dpiConn_newVar(
            self.stmt.connection.dpi_conn,
            column.oracle_type_num,
            column.native_type_num,
            self.options.batch_size,
            column.dpiVarSize(),
            0,
            0,
            null,
            &self.dpi_variables.dpi_var_array.?[ci],
            &self.dpi_variables.dpi_data_array.?[ci].?,
        ) < 0) {
            return error.FailedToCreateVariable;
        }

        // try self.conn.newVariable(
        //     column.oracle_type_num,
        //     column.native_type_num,
        //     self.options.batch_size,
        //     column.dpiVarSize(),
        //     false, // todo size_is_bytes
        //     false, // todo is_array
        //     null,
        //     &self.dpi_variables.dpi_var_array.?[ci],
        //     &self.dpi_variables.dpi_data_array.?[ci].?,
        // );
    }

    for (0..self.table_metadata.columnCount()) |i| {
        std.debug.print("i: {d}\n", .{i});
        if (c.dpiStmt_bindByPos(
            self.stmt.dpi_stmt,
            @as(u32, @intCast(i)) + 1,
            self.dpi_variables.dpi_var_array.?[i],
        ) < 0) {
            unreachable;
        }
    }
}

pub fn prepare(self: *Self) !void {
    // prepare table
    switch (self.options.mode) {
        .Append => return,
        .Truncate => try utils.truncateTable(self.conn, self.options.table),
    }

    self.table_metadata = try TableMetadata.fetch(
        self.allocator,
        self.conn,
        try self.allocator.dupe(u8, self.options.table),
    );

    if (self.options.sql) |sql| {
        self.stmt = try self.conn.prepareStatement(sql);
    } else {
        const col_names = self.options.columnNames();
        defer if (col_names) |names| self.allocator.free(names);

        const sql = try self.table_metadata.insertQuery(col_names);
        defer self.allocator.free(sql);
        self.stmt = try self.conn.prepareStatement(sql);
        std.debug.print("SQL: {s}\n", .{sql});
    }
    try self.initDpiVariables();
}
test "Writer.[prepare, resetDpiVariables]" {
    const allocator = std.testing.allocator;

    const tp = try t.getTestConnectionParams();
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
    var writer = Self.init(allocator, options);
    try writer.connect();

    try t.createTestTableIfNotExists(allocator, writer.conn, null);

    try writer.prepare();

    try t.dropTestTableIfExist(writer.conn, null);
    try writer.deinit();
}

pub fn write(self: *Self, q: *queue.MessageQueue) !void {
    var mailbox = try queue.Mailbox.init(self.allocator, self.options.batch_size);
    defer mailbox.deinit();

    break_while: while (true) {
        const node = q.get();
        switch (node.data) {
            // we are not interested in metadata here
            .Metadata => mailbox.appendMetadata(node),
            .Record => {
                mailbox.appendData(node);
                if (mailbox.isDataboxFull()) {
                    try self.writeBatch(&mailbox);
                    try self.conn.commit();
                    mailbox.resetDatabox();
                }
            },
            .Nil => {
                mailbox.appendNil(node);
                break :break_while;
            },
        }
    }
    if (mailbox.hasData()) {
        try self.writeBatch(&mailbox);
        try self.conn.commit();
        mailbox.resetDatabox();
    }

    try self.stmt.connection.commit();
}

pub fn writeBatch(self: *Self, mailbox: *queue.Mailbox) !void {
    // const dpi_var_array = &self.dpi_variables.dpi_var_array.?;
    // const dpi_data_array = &self.dpi_variables.dpi_data_array.?;

    for (0..mailbox.data_index) |ri| {
        const record = mailbox.databox[ri].*.data.Record;
        for (record.items(), 0..) |column, ci| {
            std.debug.print("\n REC {d} COLUMN {d}: {any}\n", .{ ri, ci, column });
            switch (column) {
                .Int => |val| {
                    if (val) |v| {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asDouble = @floatFromInt(v);
                    } else {
                        std.debug.print("\nxxx int null ri: {d} ci: {d}\n", .{ ri, ci });
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .Number => |val| {
                    if (val) |v| {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asDouble = v;
                    } else {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .String => |val| {
                    if (val) |v| {
                        std.debug.print("\n String value: {s}\n", .{v});
                        if (c.dpiVar_setFromBytes(self.dpi_variables.dpi_var_array.?[ci].?, @intCast(ri), v.ptr, @intCast(v.len)) < 0) {
                            std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.conn.errorMessage()});
                            unreachable;
                        }
                    } else {
                        std.debug.print("\nxxx string null ri: {d} ci: {d}\n", .{ ri, ci });
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                .Boolean => |val| {
                    if (val) |v| {
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 0;
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].value.asBoolean = if (v) 1 else 0;
                    } else {
                        std.debug.print("\nxxx bool null ri: {d} ci: {d}\n", .{ ri, ci });
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
                        std.debug.print("\nxxx ts null ri: {d} ci: {d}\n", .{ ri, ci });
                        self.dpi_variables.dpi_data_array.?[ci].?[ri].isNull = 1;
                    }
                },
                // todo
                else => unreachable,
            }
        }
    }

    std.debug.print("\nNum iters: {d}\n", .{mailbox.data_index});
    self.stmt.executeMany(mailbox.data_index) catch {
        std.debug.print("Failed to executeMany with error: {s}\n", .{self.conn.errorMessage()});
        unreachable;
    };

    try self.conn.commit();
}

test "batch-insert" {
    const allocator = std.testing.allocator;

    const tp = try t.getTestConnectionParams();
    const options = SinkOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .table = "SYS.TEST_TABLE_1",
        .mode = .Truncate,
        .batch_size = 2,
    };
    var writer = Self.init(allocator, options);
    try writer.connect();

    const create_script =
        \\CREATE TABLE SYS.TEST_TABLE_1 (
        \\    id NUMBER,
        \\    name VARCHAR2(10)
        \\)
    ;
    try utils.dropTableIfExists(writer.conn, "SYS.TEST_TABLE_1");
    try utils.executeCreateTable(writer.conn, create_script);

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
    const slq = "insert into SYS.TEST_TABLE_1 (id, name) values (:1, :2)";
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
    const s1 = "s1";
    if (c.dpiVar_setFromBytes(dpi_var_array.?[1].?, 0, s1.ptr, 2) < 0) { // Change index to 0
        std.debug.print("c.dpiVar_setFromBytes error: {s}\n", .{writer.conn.errorMessage()});
        unreachable;
    }

    // // second row
    // // set id
    // dpi_data_array.?[0].?[1].isNull = 0;
    // dpi_data_array.?[0].?[1].value.asDouble = 2;

    // // Set name
    // dpi_data_array.?[1].?[1].isNull = 0; // Add this line
    // const s2 = "s2";
    // if (c.dpiVar_setFromBytes(dpi_var_array.?[1].?, 1, s2.ptr, 2) < 0) { // Change index to 1
    //     std.debug.print("c.dpiVar_setFromBytes error: {s}\n", .{writer.conn.errorMessage()});
    //     unreachable;
    // }

    try writer.stmt.executeMany(1);
    try writer.conn.commit();

    try writer.deinit();
}

test "Writer.write" {
    const allocator = std.testing.allocator;

    const tp = try t.getTestConnectionParams();
    const options = SinkOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .table = "SYS.TEST_TABLE_WRITE_01",
        .mode = .Truncate,
        .batch_size = 2,
    };
    var writer = Self.init(allocator, options);
    try writer.connect();

    try t.dropTestTableIfExist(writer.conn, .{ .schema_dot_table = "SYS.TEST_TABLE_WRITE_01" });
    try t.createTestTableIfNotExists(allocator, writer.conn, .{
        .schema_dot_table = "SYS.TEST_TABLE_WRITE_01",
        .create_script =
        \\CREATE TABLE SYS.TEST_TABLE_WRITE_01 (
        \\    id NUMBER,
        \\    name VARCHAR2(10),
        \\    age NUMBER,
        \\    birth_date DATE,
        \\    is_active NUMBER
        \\)
        ,
    });

    try writer.prepare();

    try std.testing.expectEqual(5, writer.table_metadata.columnCount());

    var q = queue.MessageQueue.init();

    // Id, name, age, birt_date, is_active

    const record1 = Record.fromSlice(
        allocator,
        &[_]commons.Value{
            .{ .Number = 1 }, //id
            .{ .String = try allocator.dupe(u8, "John") }, //name
            .{ .Number = 20 }, //age
            .{ .TimeStamp = try zdt.Datetime.now(null) }, //birth_date
            .{ .Boolean = true }, //is_active
        },
    ) catch unreachable;
    const message1 = queue.Message{ .Record = record1 };
    const node1 = try allocator.create(queue.MessageQueue.Node);
    node1.* = .{ .data = message1 };
    q.put(node1);

    const record2 = Record.fromSlice(allocator, &[_]commons.Value{
        .{ .Int = 2 }, //id
        .{ .String = try allocator.dupe(u8, "Jane") }, //name
        .{ .Int = 21 }, //age
        .{ .TimeStamp = try zdt.Datetime.now(null) }, //birth_date
        .{ .Boolean = false }, //is_active
    }) catch unreachable;
    const message2 = queue.Message{ .Record = record2 };
    const node2 = try allocator.create(queue.MessageQueue.Node);
    node2.* = .{ .data = message2 };
    q.put(node2);

    const term = try allocator.create(queue.MessageQueue.Node);
    term.* = .{ .data = .Nil };
    q.put(term);

    try writer.write(&q);
    errdefer {
        std.debug.print("Error in writer.write with error: {s}\n", .{writer.conn.errorMessage()});
    }

    try writer.stmt.connection.commit();
    try writer.deinit();

    // try t.dropTestTableIfExist(writer.conn, null);
}
