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

const t = @import("../testing/testing.zig");
const Self = @This();

pub const Table = struct {
    schema: []const u8,
    name: []const u8,
};

allocator: Allocator,
table_name: []const u8,

columns: []Column = undefined,
table: Table = undefined,

pub fn toTable(table_name: []const u8) Table {
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

test "toTable" {
    const table1 = Self.toTable("SCHEMA.TABLE");
    try std.testing.expectEqualStrings("SCHEMA", table1.schema);
    try std.testing.expectEqualStrings("TABLE", table1.name);

    const table2 = Self.toTable("TABLE");
    try std.testing.expectEqualStrings("", table2.schema);
    try std.testing.expectEqualStrings("TABLE", table2.name);
}

pub fn init(allocator: Allocator, table_name: []const u8) Self {
    return .{
        .allocator = allocator,
        .table_name = table_name,
        .table = toTable(table_name),
    };
}

pub fn findMetadata(self: *Self, connection: *Connection) !void {
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
        const index = map.get("column_id").?.Int;
        const name = map.get("column_name").?.String;
        const nullable = map.get("nullable").?.String.?[0] == 'Y';
        const data_type = map.get("data_type").?.String;
        const length = map.get("data_length").?.Int;
        const precision = map.get("data_precision").?.Int;
        const scale = map.get("data_scale").?.Int;
        const default = map.get("data_default").?.String;

        try columns.append(.{
            .index = @intCast(index.?),
            .name = name.?,
            .nullable = nullable,
            .oracle_type_num = utils.toDpiOracleTypeNum(data_type.?),
            .native_type_num = utils.toDpiNativeTypeNum(data_type.?),
            .length = @intCast(length.?),
            .precision = if (precision) |p| @intCast(p) else null,
            .scale = if (scale) |s| @intCast(s) else null,
            .default = default,
        });
    }
    self.columns = try columns.toOwnedSlice();
}

test findMetadata {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const create_script =
        \\CREATE TABLE "TEST_TABLE" (
        \\  "ID" NUMBER(10) NOT NULL,
        \\  "NAME" VARCHAR2(50) NOT NULL,
        \\  "AGE" NUMBER(3) NOT NULL,
        \\  "BIRTH_DATE" DATE NOT NULL,
        \\  "IS_ACTIVE" NUMBER(1) NOT NULL
        \\)
    ;
    var table = Self.init(allocator, "TEST_TABLE");
    {
        var conn = try t.getTestConnection(allocator);
        try conn.connect();

        errdefer {
            std.debug.print("Error: {s}\n", .{conn.errorMessage()});
        }

        try utils.dropTableIfExists(&conn, "TEST_TABLE");
        _ = try conn.execute(create_script);

        try table.findMetadata(&conn);

        try utils.dropTableIfExists(&conn, "TEST_TABLE");
    }
}
