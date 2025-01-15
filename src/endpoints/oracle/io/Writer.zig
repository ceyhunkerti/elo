const std = @import("std");
const Connection = @import("../Connection.zig");
const SinkOptions = @import("../options.zig").SinkOptions;
const MessageQueue = @import("../../../queue.zig").MessageQueue;
const TableMetadata = @import("../metadata/TableMetadata.zig");
const t = @import("../testing/testing.zig");

const utils = @import("../utils.zig");

const Self = @This();

allocator: std.mem.Allocator,
conn: *Connection = undefined,
options: SinkOptions,

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
}

pub fn connect(self: Self) !void {
    return try self.conn.connect();
}

pub fn buildInsertQuery(self: Self) ![]const u8 {
    if (self.options.sql) |sql| {
        return sql;
    }
    const table_name = self.options.table.?;
    var tmd = try TableMetadata.init(self.allocator, table_name, self.conn);
    defer tmd.deinit();

    var column_names = try self.allocator.alloc([]const u8, tmd.columns.len);
    defer self.allocator.free(column_names);
    for (tmd.columns, 0..) |column, i| {
        column_names[i] = column.name;
    }

    var bindings = std.ArrayList([]const u8).init(self.allocator);
    for (0..tmd.columns.len) |i| {
        try bindings.append(try std.fmt.allocPrint(self.allocator, ":{d}", .{i + 1}));
    }
    defer {
        for (bindings.items) |b| {
            self.allocator.free(b);
        }
        bindings.deinit();
    }
    return try std.fmt.allocPrint(self.allocator, "insert into {s} ({s}) values ({s})", .{
        table_name,
        try std.mem.join(self.allocator, ",", column_names),
        try std.mem.join(self.allocator, ",", bindings.items),
    });
}

test "Writer.buildInsertQuery" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{t.schema()});

    const create_script = try std.fmt.allocPrint(allocator,
        \\CREATE TABLE {s} (
        \\  ID NUMBER(10) NOT NULL,
        \\  NAME VARCHAR2(50) NOT NULL,
        \\  AGE NUMBER(3) NOT NULL,
        \\  BIRTH_DATE DATE NOT NULL,
        \\  IS_ACTIVE NUMBER(1) NOT NULL
        \\)
    , .{schema_dot_table});

    var conn = try t.getTestConnection(allocator);
    try conn.connect();

    errdefer {
        std.debug.print("Error: {s}\n", .{conn.errorMessage()});
    }

    try utils.dropTableIfExists(&conn, schema_dot_table);
    _ = try conn.execute(create_script);

    const tp = try t.getTestConnectionParams();
    const options = SinkOptions{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .privilege = tp.privilege,
        },
        .table = schema_dot_table,
    };

    var writer = Self.init(allocator, options);
    try writer.connect();
    errdefer {
        std.debug.print("Writer Error: {s}\n", .{writer.conn.errorMessage()});
    }

    const query = try writer.buildInsertQuery();
    try std.testing.expectEqualStrings(
        query,
        try std.fmt.allocPrint(
            allocator,
            "insert into {s} (ID,NAME,AGE,BIRTH_DATE,IS_ACTIVE) values (:1,:2,:3,:4,:5)",
            .{schema_dot_table},
        ),
    );

    try utils.dropTableIfExists(&conn, schema_dot_table);
}
