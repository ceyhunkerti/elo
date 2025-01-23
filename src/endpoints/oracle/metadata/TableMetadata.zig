const std = @import("std");
const Statement = @import("../Statement.zig");
const Column = @import("./Column.zig");
const Connection = @import("../Connection.zig");
const utils = @import("../utils.zig");

const oci = @import("../c.zig").c;
const p = @import("../../../wire/proto.zig");
const t = @import("../testing/testing.zig");

const Self = @This();

pub const Table = struct {
    allocator: std.mem.Allocator,
    raw_name: []const u8,
    schema: []const u8,
    name: []const u8,

    pub fn init(allocator: std.mem.Allocator, table_name: []const u8) Table {
        const raw_name = allocator.dupe(u8, table_name) catch unreachable;

        var tokens = std.mem.split(u8, raw_name, ".");
        const schema_or_table = tokens.first();
        const name = tokens.rest();

        if (name.len == 0) {
            return .{
                .allocator = allocator,
                .raw_name = raw_name,
                .schema = "",
                .name = schema_or_table,
            };
        } else {
            return .{
                .allocator = allocator,
                .raw_name = raw_name,
                .schema = schema_or_table,
                .name = name,
            };
        }
    }

    pub fn deinit(self: Table) void {
        self.allocator.free(self.raw_name);
    }
};

allocator: std.mem.Allocator = undefined,
table: Table = undefined,
columns: ?[]Column = null,

pub fn deinit(self: Self) void {
    self.table.deinit();
    if (self.columns) |columns| {
        for (columns) |*column| {
            column.deinit();
        }
        self.allocator.free(columns);
    }
}

pub fn fetch(allocator: std.mem.Allocator, conn: *Connection, table_name: []const u8) !Self {
    const table = Table.init(allocator, table_name);

    const sql = try std.fmt.allocPrint(
        allocator,
        \\select
        \\  c.column_name,
        \\  c.data_type,
        \\  c.data_length,
        \\  c.data_precision,
        \\  c.data_scale,
        \\  c.nullable,
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
    defer stmt.deinit() catch unreachable;
    try stmt.execute();

    var columns = std.ArrayList(Column).init(allocator);
    const column_names = &[_][]const u8{
        "column_name",
        "data_type",
        "data_length",
        "data_precision",
        "data_scale",
        "nullable",
        "column_id",
    };
    var rs = try stmt.getResultSet();
    defer rs.deinit();

    var it = rs.iterator();
    while (try it.next()) |record| {
        var map = try record.asMap(allocator, column_names);
        defer map.deinit();
        defer record.deinit(allocator);

        const index = map.get("column_id").?.Double;
        const name = map.get("column_name").?.String;
        const nullable = map.get("nullable").?.String.?[0] == 'Y';
        const slq_type = map.get("data_type").?.String;
        const size = map.get("data_length").?.Double;
        const scale = map.get("data_scale").?.Double;
        const precision = map.get("data_precision").?.Double;

        try columns.append(Column{
            .allocator = allocator,
            .index = std.math.lossyCast(u32, index.?),
            .name = try allocator.dupe(u8, name.?),
            .nullable = nullable,
            .sql_type = try allocator.dupe(u8, slq_type.?),
            .size = std.math.lossyCast(u32, size.?),
            .scale = std.math.lossyCast(i32, scale.?),
            .precision = std.math.lossyCast(i32, precision.?),
        });
    }

    return .{
        .allocator = allocator,
        .table = table,
        .columns = try columns.toOwnedSlice(),
    };
}
test "TableMetadata.fetch" {
    const allocator = std.testing.allocator;
    const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{t.schema()});
    defer allocator.free(schema_dot_table);

    const create_script = try std.fmt.allocPrint(allocator,
        \\CREATE TABLE {s} (
        \\  ID NUMBER(10) NOT NULL,
        \\  NAME VARCHAR2(50) NOT NULL,
        \\  AGE NUMBER(3) NOT NULL,
        \\  BIRTH_DATE DATE NOT NULL,
        \\  IS_ACTIVE NUMBER(1) NOT NULL
        \\)
    , .{schema_dot_table});
    defer allocator.free(create_script);

    var conn = try t.getConnection(allocator);
    try conn.connect();
    defer conn.deinit() catch unreachable;

    try utils.dropTableIfExists(&conn, schema_dot_table);
    _ = try conn.execute(create_script);

    errdefer conn.printLastError();

    var tmd = try Self.fetch(allocator, &conn, schema_dot_table);

    defer tmd.deinit();

    try std.testing.expectEqual(tmd.columns.?.len, 5);
    try std.testing.expectEqualStrings(tmd.columns.?[0].name, "ID");
    try std.testing.expectEqualStrings(tmd.columns.?[1].name, "NAME");
    try std.testing.expectEqualStrings(tmd.columns.?[2].name, "AGE");
    try std.testing.expectEqualStrings(tmd.columns.?[3].name, "BIRTH_DATE");
    try std.testing.expectEqualStrings(tmd.columns.?[4].name, "IS_ACTIVE");

    try std.testing.expectEqual(tmd.columns.?[0].index, 1);
    try std.testing.expectEqual(tmd.columns.?[0].size, 22);
    try std.testing.expectEqual(tmd.columns.?[0].precision, 10);
    try std.testing.expectEqual(tmd.columns.?[0].scale, 0);
    try std.testing.expectEqual(tmd.columns.?[0].nullable, false);
    try std.testing.expectEqualStrings(tmd.columns.?[0].sql_type, "NUMBER");

    const col_str = try tmd.columns.?[0].toString();
    defer allocator.free(col_str);
    try std.testing.expectEqualStrings(
        \\Name: ID
        \\OracleTypeNme: NUMBER
        \\OracleTypeNum: 2010
        \\NativeTypeNum: 3003
        \\Nullable: false
    , col_str);
}

pub fn insertQuery(self: Self, columns: ?[]const []const u8) ![]const u8 {
    var column_names = std.ArrayList([]const u8).init(self.allocator);
    defer column_names.deinit();

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
            for (self.columns.?) |column| {
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
        for (self.columns.?) |column| {
            i += 1;
            const b = try std.fmt.allocPrint(self.allocator, ":{d}", .{i});
            try bindings.append(b);
            try column_names.append(column.name);
        }
    }

    const columns_expression = try std.mem.join(self.allocator, ",", column_names.items);
    defer self.allocator.free(columns_expression);
    const bindings_expression = try std.mem.join(self.allocator, ",", bindings.items);
    defer self.allocator.free(bindings_expression);

    const sql = try std.fmt.allocPrint(self.allocator,
        \\INSERT INTO {s} ({s}) VALUES ({s})
    , .{
        self.table.raw_name,
        columns_expression,
        bindings_expression,
    });
    return sql;
}
// test "TableMetadata.buildInsertQuery" {
//     const allocator = std.testing.allocator;
//     const schema_dot_table = try std.fmt.allocPrint(allocator, "{s}.TEST_TABLE", .{t.schema()});

//     const create_script = try std.fmt.allocPrint(allocator,
//         \\CREATE TABLE {s} (
//         \\  ID NUMBER(10) NOT NULL,
//         \\  NAME VARCHAR2(50) NOT NULL,
//         \\  AGE NUMBER(3) NOT NULL,
//         \\  BIRTH_DATE DATE NOT NULL,
//         \\  IS_ACTIVE NUMBER(1) NOT NULL
//         \\)
//     , .{schema_dot_table});
//     defer allocator.free(create_script);

//     var conn = try t.getTestConnection(allocator);
//     try conn.connect();

//     errdefer {
//         conn.printLastError();
//     }

//     try utils.dropTableIfExists(&conn, schema_dot_table);
//     _ = try conn.execute(create_script);

//     var tmd = try Self.fetch(allocator, &conn, schema_dot_table);
//     defer {
//         tmd.deinit();
//     }

//     const insert_query = try tmd.insertQuery(null);
//     defer allocator.free(insert_query);

//     try std.testing.expectEqualStrings(
//         insert_query,
//         "INSERT INTO sys.TEST_TABLE (ID,NAME,AGE,BIRTH_DATE,IS_ACTIVE) VALUES (:1,:2,:3,:4,:5)",
//     );

//     const insert_query_2 = try tmd.insertQuery(&[_][]const u8{ "ID", "NAME" });
//     defer allocator.free(insert_query_2);

//     try std.testing.expectEqualStrings(
//         insert_query_2,
//         "INSERT INTO sys.TEST_TABLE (ID,NAME) VALUES (:1,:2)",
//     );

//     try conn.deinit();
// }

pub fn columnCount(self: Self) usize {
    if (self.columns) |columns| return columns.len;
    return 0;
}
