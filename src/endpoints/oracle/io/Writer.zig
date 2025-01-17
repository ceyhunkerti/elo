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

const Self = @This();

allocator: std.mem.Allocator,
conn: *Connection = undefined,
options: SinkOptions,

batch_index: u32 = 0,
table_metadata: TableMetadata = undefined,
dpi_variables: struct {
    dpi_var_array: ?[]?*c.dpiVar = null,
    dpi_data_array: ?[]?[*c]c.dpiData = null,
} = .{},

stmt: *Statement = null,

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

pub fn clearDpiVariables(self: *Self) !void {
    if (self.dpi_variables.dpi_var_array) |arr| for (arr) |var_| {
        if (var_) |v| {
            if (c.dpiVar_release(v) > 0) {
                std.debug.print("Failed to release variable with error: {s}\n", .{self.conn.errorMessage()});
                return error.FailedToClearDpiVariables;
            }
        }
    };
}

pub fn resetDpiVariables(self: *Self, array_size: u32) !void {
    self.dpi_variables.dpi_var_array = try self.allocator.alloc(?*c.dpiVar, self.table_metadata.columns.len);
    self.dpi_variables.dpi_data_array = try self.allocator.alloc(?[*c]c.dpiData, self.options.batch_size);

    for (self.table_metadata.columns, 0..) |column, i| {
        try self.conn.newVariable(
            c.DPI_ORACLE_TYPE_NUMBER,
            c.DPI_NATIVE_TYPE_INT64,
            array_size,
            column.dpiVarSize(),
            false, // todo size_is_bytes
            false, // todo is_array
            null,
            &self.dpi_variables.dpi_var_array.?[i],
            &self.dpi_variables.dpi_data_array.?[i].?,
        );
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
        const sql = try self.table_metadata.insertQuery(self.options.columns);
        defer self.allocator.free(sql);
        self.stmt = try self.conn.prepareStatement(sql);
    }
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

    try writer.resetDpiVariables(writer.options.batch_size);
    try writer.clearDpiVariables();

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
        mailbox.resetDatabox();
    }
}

pub fn writeBatch(self: *Self, mailbox: *queue.Mailbox) !void {
    try self.resetDpiVariables(mailbox.data_index);

    const dpi_var_array = self.dpi_variables.dpi_var_array.?;
    const dpi_data_array = self.dpi_variables.dpi_data_array.?;

    for (0..mailbox.data_index) |ri| {
        const record = mailbox.databox[ri].*.data.Record;
        for (record.items, 0..) |column, ci| {
            switch (column) {
                .Int => |val| {
                    if (val) |v| {
                        dpi_data_array[ri].?[ci].isNull = 0;
                        dpi_data_array[ri].?[ci].value.asInt64 = v;
                    } else {
                        dpi_data_array[ri].?[ci].isNull = 1;
                    }
                },
                .Number => |val| {
                    if (val) |v| {
                        dpi_data_array[ri].?[ci].isNull = 0;
                        dpi_data_array[ri].?[ci].value.asDouble = v;
                    } else {
                        dpi_data_array[ri].?[ci].isNull = 1;
                    }
                },
                .String => |val| {
                    if (val) |v| {
                        if (c.dpiVar_setFromBytes(dpi_var_array[ci].?, ri, v.ptr, v.len) < 0) {
                            std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.conn.errorMessage()});
                            unreachable;
                        } else {
                            dpi_data_array[ri].?[ci].isNull = 0;
                        }
                    }
                },
                // todo
                else => unreachable,
            }
        }
    }
    try self.stmt.executeMany(mailbox.data_index);
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
        .table = "TEST_TABLE",
        .mode = .Truncate,
        .batch_size = 2,
    };
    var writer = Self.init(allocator, options);
    try writer.connect();
    try writer.prepare();

    var q = queue.MessageQueue.init();
    const record1 = Record.fromSlice(allocator, &[_]commons.Value{ .{ .Int = 1 }, .{ .Boolean = true } }) catch unreachable;
    const message1 = queue.Message{ .Record = record1 };
    const node1 = try allocator.create(queue.MessageQueue.Node);
    node1.* = .{ .data = message1 };
    q.put(node1);

    const record2 = Record.fromSlice(allocator, &[_]commons.Value{ .{ .Int = 2 }, .{ .Boolean = false } }) catch unreachable;
    const message2 = queue.Message{ .Record = record2 };
    const node2 = try allocator.create(queue.MessageQueue.Node);
    node2.* = .{ .data = message2 };
    q.put(node2);

    const term = try allocator.create(queue.MessageQueue.Node);
    term.* = .{ .data = .Nil };
    q.put(term);

    try writer.write(&q);

    try t.dropTestTableIfExist(writer.conn, null);
    try writer.deinit();
}
