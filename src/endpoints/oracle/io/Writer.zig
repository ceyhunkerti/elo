const c = @import("../c.zig").c;
const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const queue = @import("../../../queue.zig");
const TableMetadata = @import("../metadata/TableMetadata.zig");
const CreateTableScript = @import("../metadata/script.zig").CreateTableScript;
const commons = @import("../../../commons.zig");
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

pub fn resetDpiVariables(self: *Self) !void {
    self.dpi_variables.dpi_var_array = try self.allocator.alloc(?*c.dpiVar, self.table_metadata.columns.len);
    self.dpi_variables.dpi_data_array = try self.allocator.alloc(?[*c]c.dpiData, self.options.batch_size);

    for (self.table_metadata.columns, 0..) |column, i| {
        try self.conn.newVariable(
            c.DPI_ORACLE_TYPE_NUMBER,
            c.DPI_NATIVE_TYPE_INT64,
            self.options.batch_size,
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

    try writer.resetDpiVariables();
    try writer.clearDpiVariables();

    try t.dropTestTableIfExist(writer.conn, null);
    try writer.deinit();
}

// pub fn write(self: *Self, q: *queue.MessageQueue) !void {
//     break_while: while (true) {
//         const node = q.get() orelse break;
//         switch (node.data) {
//             // we are not interested in metadata here
//             .Metadata => {},
//             .Record => |record| {
//                 _ = record;
//             },
//             .Nil => break :break_while,
//         }
//     }
// }

// pub inline fn addToBatch(self: *Self, record: Record) !void {}
