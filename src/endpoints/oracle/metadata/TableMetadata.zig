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
table: Table = undefined,
columns: []Column = undefined,

pub fn deinit(self: *Self) void {
    self.allocator.free(self.table_name);
    self.allocator.free(self.columns);
}

pub fn fetch(allocator: Allocator, conn: *Connection, table_name: []const u8) !Self {
    const table = Table.init(table_name);
    const sql = try std.fmt.allocPrint(
        allocator,
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
            table.name,
            table.schema,
        },
    );
    defer allocator.free(sql);

    var stmt = try conn.prepareStatement(sql);
    const column_count = try stmt.execute();
    var columns = std.ArrayList(Column).init(allocator);
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
        const record = try stmt.fetch(column_count) orelse break;
        var map: std.StringHashMap(FieldValue) = RecordAsMap(
            allocator,
            column_names,
            record,
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
            .name = try allocator.dupe(u8, name.?),
            .nullable = nullable,
            .oracle_type_num = utils.toDpiOracleTypeNum(data_type.?),
            .native_type_num = utils.toDpiNativeTypeNum(data_type.?),
            .length = std.math.lossyCast(u32, length.?),
            .precision = if (precision) |p| std.math.lossyCast(u32, p) else null,
            .scale = if (scale) |s| std.math.lossyCast(u32, s) else null,
            .default = if (default) |d| try allocator.dupe(u8, d) else null,
        });
        allocator.free(record);
        map.deinit();
    }
    return .{
        .allocator = allocator,
        .table_name = try allocator.dupe(u8, table_name),
        .table = table,
        .columns = try columns.toOwnedSlice(),
    };
}
test "TableMetadata.fetch" {
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

    const table = try Self.fetch(allocator, &conn, schema_dot_table);

    try utils.dropTableIfExists(&conn, schema_dot_table);

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

pub fn insertQuery(self: Self, columns: ?[]const []const u8) ![]const u8 {
    var column_names = std.ArrayList(Column).init(self.allocator);
    defer self.allocator.destroy(column_names);

    var bindings = std.ArrayList([]const u8).init(self.allocator);
    defer {
        for (bindings.items) |b| {
            self.allocator.free(b);
        }
        bindings.deinit();
    }

    var i: usize = 0;
    if (columns) |cols| {
        for (cols) |name| {
            for (self.columns) |column| {
                if (std.mem.eql(u8, column.name, name)) {
                    i += 1;
                    const b = try std.fmt.allocPrint(self.allocator, ":{d}", .{i});
                    try bindings.append(b);
                    try column_names.append(name);
                    break;
                }
            }
        }
    } else {
        for (self.columns) |column| {
            i += 1;
            const b = try std.fmt.allocPrint(self.allocator, ":{d}", .{i});
            try bindings.append(b);
            try column_names.append(column.name);
        }
    }

    const sql = try std.fmt.allocPrint(self.allocator,
        \\INSERT INTO {s} ({s})
        \\VALUES ({s})
    , .{
        self.table_name,
        try column_names.join(self.allocator, ","),
        try bindings.join(self.allocator, ","),
    });
    return sql;
}
test "TableMetadata.buildInsertQuery" {
    // const allocator = std.testing.allocator;
    // const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{t.schema()});
    // defer allocator.free(schema_dot_table);
    // const create_script = try std.fmt.allocPrint(allocator,
    //     \\CREATE TABLE {s} (
    //     \\  ID NUMBER(10) NOT NULL,
    //     \\  NAME VARCHAR2(50) NOT NULL,
    //     \\  AGE NUMBER(3) NOT NULL,
    //     \\  BIRTH_DATE DATE NOT NULL,
    //     \\  IS_ACTIVE NUMBER(1) NOT NULL
    //     \\)
    // , .{schema_dot_table});
    // defer allocator.free(create_script);

    // var conn = try t.getTestConnection(allocator);
    // try conn.connect();
    // // allocator.destroy(conn);

    // // errdefer {
    // //     std.debug.print("Error: {s}\n", .{conn.errorMessage()});
    // // }

    // try utils.dropTableIfExists(&conn, schema_dot_table);
    // _ = try conn.execute(create_script);

    // var table = try Self.fetch(allocator, &conn, schema_dot_table);
    // defer table.deinit();

    // try utils.dropTableIfExists(&conn, schema_dot_table);

    // try std.testing.expectEqual(table.columns.len, 5);
    // try std.testing.expectEqualStrings(table.columns[0].name, "ID");
    // try std.testing.expectEqualStrings(table.columns[1].name, "NAME");
    // try std.testing.expectEqualStrings(table.columns[2].name, "AGE");
    // try std.testing.expectEqualStrings(table.columns[3].name, "BIRTH_DATE");
    // try std.testing.expectEqualStrings(table.columns[4].name, "IS_ACTIVE");

    // try std.testing.expectEqual(table.columns[0].oracle_type_num, c.DPI_ORACLE_TYPE_NUMBER);
    // try std.testing.expectEqual(table.columns[0].native_type_num, c.DPI_NATIVE_TYPE_DOUBLE);
    // try std.testing.expectEqual(table.columns[0].length, 22);
    // try std.testing.expectEqual(table.columns[0].precision, 10);
    // try std.testing.expectEqual(table.columns[0].scale, 0);
    // try std.testing.expectEqual(table.columns[0].nullable, false);
}
