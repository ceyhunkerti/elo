const std = @import("std");
const Allocator = std.mem.Allocator;
const Statement = @import("../Statement.zig");
const Column = @import("./Column.zig");
const Connection = @import("../Connection.zig");
const commons = @import("../../../commons.zig");
const Record = commons.Record;
const FieldValue = commons.FieldValue;
const RecordAsMap = commons.RecordAsMap;
const utils = @import("../utils.zig");
const c = @import("../c.zig").c;

const t = @import("../testing/testing.zig");
const Self = @This();

pub const Table = struct {
    schema: []const u8,
    name: []const u8,
    pub fn init(table_name: []const u8) Table {
        var tokens = std.mem.split(u8, table_name, ".");
        const schema_or_table = tokens.first();
        const name = tokens.rest();

        if (name.len == 0) {
            return .{
                .schema = "",
                .name = schema_or_table,
            };
        } else {
            return .{
                .schema = schema_or_table,
                .name = name,
            };
        }
    }
};

allocator: Allocator,
table_name: []const u8,

columns: []Column = undefined,
table: Table = undefined,

test "Table.init" {
    const table1 = Table.init("SCHEMA.TABLE");
    try std.testing.expectEqualStrings("SCHEMA", table1.schema);
    try std.testing.expectEqualStrings("TABLE", table1.name);

    const table2 = Table.init("TABLE");
    try std.testing.expectEqualStrings("", table2.schema);
    try std.testing.expectEqualStrings("TABLE", table2.name);
}

pub fn init(allocator: Allocator, table_name: []const u8, conn: *Connection) !Self {
    var self = Self{
        .allocator = allocator,
        .table_name = table_name,
        .table = Table.init(table_name),
    };

    try self.load(conn);
    return self;
}

pub fn deinit(self: *Self) void {
    for (self.columns) |*column| {
        column.deinit();
    }
    self.allocator.free(self.columns);
    self.allocator.destroy(self);
}

pub fn load(self: *Self, connection: *Connection) !void {
    const sql = try std.fmt.allocPrint(
        self.allocator,
        \\select
        \\  c.column_name,
        \\  c.data_type,
        \\  c.data_length,
        \\  c.data_precision,
        \\  c.data_scale,
        \\  c.nullable,
        \\  c.data_default,
        \\  c.column_id
        \\from  all_tables t, all_tab_cols c
        \\where t.table_name = c.table_name and t.table_name = upper('{s}') and t.owner = upper('{s}')
    ,
        .{
            self.table.name,
            self.table.schema,
        },
    );
    defer self.allocator.free(sql);
    // std.debug.print("SQL: {s}\n", .{sql});

    var stmt = try connection.prepareStatement(sql);
    const column_count = try stmt.execute();
    var columns = std.ArrayList(Column).init(self.allocator);
    const column_names = &[_][]const u8{
        "column_name",
        "data_type",
        "data_length",
        "data_precision",
        "data_scale",
        "nullable",
        "data_default",
        "column_id",
    };
    while (true) {
        const map: std.StringHashMap(FieldValue) = RecordAsMap(
            self.allocator,
            column_names,
            try stmt.fetch(column_count) orelse break,
        );
        const index = map.get("column_id").?.Double;
        const name = map.get("column_name").?.String;
        const nullable = map.get("nullable").?.String.?[0] == 'Y';
        const data_type = map.get("data_type").?.String;
        const length = map.get("data_length").?.Double;
        const precision = map.get("data_precision").?.Double;
        const scale = map.get("data_scale").?.Double;
        const default = map.get("data_default").?.String;

        try columns.append(.{
            .index = std.math.lossyCast(u32, index.?),
            .name = name.?,
            .nullable = nullable,
            .oracle_type_num = utils.toDpiOracleTypeNum(data_type.?),
            .native_type_num = utils.toDpiNativeTypeNum(data_type.?),
            .length = std.math.lossyCast(u32, length.?),
            .precision = if (precision) |p| std.math.lossyCast(u32, p) else null,
            .scale = if (scale) |s| std.math.lossyCast(u32, s) else null,
            .default = default,
        });
    }
    self.columns = try columns.toOwnedSlice();
}

test "TableMetadata.load" {
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

    var table = Self{ .allocator = allocator, .table_name = schema_dot_table, .table = Table.init(schema_dot_table) };
    {
        var conn = try t.getTestConnection(allocator);
        try conn.connect();

        errdefer {
            std.debug.print("Error: {s}\n", .{conn.errorMessage()});
        }

        try utils.dropTableIfExists(&conn, schema_dot_table);
        _ = try conn.execute(create_script);

        try table.load(&conn);

        try utils.dropTableIfExists(&conn, schema_dot_table);
    }
    try std.testing.expectEqual(table.columns.len, 5);
    try std.testing.expectEqualStrings(table.columns[0].name, "ID");
    try std.testing.expectEqualStrings(table.columns[1].name, "NAME");
    try std.testing.expectEqualStrings(table.columns[2].name, "AGE");
    try std.testing.expectEqualStrings(table.columns[3].name, "BIRTH_DATE");
    try std.testing.expectEqualStrings(table.columns[4].name, "IS_ACTIVE");

    try std.testing.expectEqual(table.columns[0].oracle_type_num, c.DPI_ORACLE_TYPE_NUMBER);
    try std.testing.expectEqual(table.columns[0].native_type_num, c.DPI_NATIVE_TYPE_DOUBLE);
    try std.testing.expectEqual(table.columns[0].length, 22);
    try std.testing.expectEqual(table.columns[0].precision, 10);
    try std.testing.expectEqual(table.columns[0].scale, 0);
    try std.testing.expectEqual(table.columns[0].nullable, false);
}
