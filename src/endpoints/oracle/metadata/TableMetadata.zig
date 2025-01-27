const TableMetadata = @This();

const std = @import("std");
const Statement = @import("../Statement.zig");
const Column = @import("./Column.zig");
const Connection = @import("../Connection.zig");
const utils = @import("../utils.zig");

const shared = @import("../../shared.zig");
const TableName = shared.TableName;

const p = @import("../../../wire/proto/proto.zig");
const c = @import("../c.zig").c;
const e = @import("../error.zig");
const t = @import("../testing/testing.zig");

pub const Error = error{
    FailedToGetObjectType,
    FailedToGetObjectTypeInfo,
};

allocator: std.mem.Allocator = undefined,
table: TableName = undefined,
columns: ?[]Column = null,

pub fn deinit(self: TableMetadata) void {
    self.table.deinit();
    if (self.columns) |columns| {
        for (columns) |*column| {
            column.deinit();
        }
        self.allocator.free(columns);
    }
}

pub fn fetch(allocator: std.mem.Allocator, conn: *Connection, table_name: []const u8) !TableMetadata {
    const table = try TableName.init(allocator, table_name, conn.username);

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
            table.tablename,
            table.schema,
        },
    );
    defer allocator.free(sql);
    // std.debug.print("SQL: {s}\n", .{sql});

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
        var map = try record.asMap(allocator, column_names);
        defer map.deinit();
        defer record.deinit(allocator);

        const index = map.get("column_id").?.Double;
        const name = map.get("column_name").?.String;
        const nullable = map.get("nullable").?.String.?[0] == 'Y';
        const data_type = map.get("data_type").?.String;
        const length = map.get("data_length").?.Double;
        const precision = map.get("data_precision").?.Double;
        const scale = map.get("data_scale").?.Double;
        const default = map.get("data_default").?.String;

        try columns.append(.{
            .allocator = allocator,
            .index = std.math.lossyCast(u32, index.?),
            .name = try allocator.dupe(u8, name.?),
            .nullable = nullable,
            .dpi_oracle_type_num = utils.toDpiOracleTypeNum(data_type.?),
            .dpi_native_type_num = utils.toDpiNativeTypeNum(data_type.?),
            .oracle_type_name = if (data_type) |d| try allocator.dupe(u8, d) else null,
            .length = std.math.lossyCast(u32, length.?),
            .precision = if (precision) |precision_| std.math.lossyCast(u32, precision_) else null,
            .scale = if (scale) |scale_| std.math.lossyCast(u32, scale_) else null,
            .default = if (default) |default_| try allocator.dupe(u8, default_) else null,
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
    const table_name = "TEST_TABLE_METADATA_01";
    const create_script =
        \\CREATE TABLE {name} (
        \\  ID NUMBER(10) NOT NULL,
        \\  NAME VARCHAR2(50) NOT NULL,
        \\  AGE NUMBER(3) NOT NULL,
        \\  BIRTH_DATE DATE NOT NULL,
        \\  IS_ACTIVE NUMBER(1) NOT NULL
        \\)
    ;

    var conn = t.connection(allocator);
    defer conn.deinit() catch unreachable;
    try conn.connect();
    errdefer {
        std.debug.print("Error: {s}\n", .{conn.errorMessage()});
    }
    const tt = t.TestTable.init(allocator, &conn, table_name, create_script);
    defer {
        tt.dropIfExists() catch unreachable;
        tt.deinit();
    }
    try tt.createIfNotExists();

    var tmd = try TableMetadata.fetch(allocator, &conn, tt.name());
    defer tmd.deinit();

    try std.testing.expectEqual(tmd.columns.?.len, 5);
    try std.testing.expectEqualStrings(tmd.columns.?[0].name, "ID");
    try std.testing.expectEqualStrings(tmd.columns.?[1].name, "NAME");
    try std.testing.expectEqualStrings(tmd.columns.?[2].name, "AGE");
    try std.testing.expectEqualStrings(tmd.columns.?[3].name, "BIRTH_DATE");
    try std.testing.expectEqualStrings(tmd.columns.?[4].name, "IS_ACTIVE");

    try std.testing.expectEqual(tmd.columns.?[0].dpi_oracle_type_num, c.DPI_ORACLE_TYPE_NUMBER);
    try std.testing.expectEqual(tmd.columns.?[0].dpi_native_type_num, c.DPI_NATIVE_TYPE_DOUBLE);
    try std.testing.expectEqual(tmd.columns.?[0].length, 22);
    try std.testing.expectEqual(tmd.columns.?[0].precision, 10);
    try std.testing.expectEqual(tmd.columns.?[0].scale, 0);
    try std.testing.expectEqual(tmd.columns.?[0].nullable, false);
    try std.testing.expectEqualStrings(tmd.columns.?[0].oracle_type_name.?, "NUMBER");

    const col_str = try tmd.columns.?[0].toString();
    defer allocator.free(col_str);
    try std.testing.expectEqualStrings(
        \\Name: ID
        \\OracleTypeNme: NUMBER
        \\OracleTypeNum: 2010
        \\NativeTypeNum: 3003
        \\Nullable: false
    ,
        col_str,
    );
}

pub fn insertQuery(self: TableMetadata, columns: ?[]const []const u8) ![]const u8 {
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
        self.table.name,
        columns_expression,
        bindings_expression,
    });
    return sql;
}
test "TableMetadata.buildInsertQuery" {
    const allocator = std.testing.allocator;
    const table_name = "TEST_BUILD_INS_QUERY";
    const create_script =
        \\CREATE TABLE {name} (
        \\  ID NUMBER(10) NOT NULL,
        \\  NAME VARCHAR2(50) NOT NULL,
        \\  AGE NUMBER(3) NOT NULL,
        \\  BIRTH_DATE DATE NOT NULL,
        \\  IS_ACTIVE NUMBER(1) NOT NULL
        \\)
    ;

    var conn = t.connection(allocator);
    defer conn.deinit() catch unreachable;
    try conn.connect();
    errdefer {
        std.debug.print("Error: {s}\n", .{conn.errorMessage()});
    }
    const tt = t.TestTable.init(allocator, &conn, table_name, create_script);
    defer {
        tt.dropIfExists() catch unreachable;
        tt.deinit();
    }
    try tt.createIfNotExists();

    var tmd = try TableMetadata.fetch(allocator, &conn, tt.tablename());
    defer tmd.deinit();

    const insert_query = try tmd.insertQuery(null);
    defer allocator.free(insert_query);
    const q1 = try std.fmt.allocPrint(
        allocator,
        "INSERT INTO {s} (ID,NAME,AGE,BIRTH_DATE,IS_ACTIVE) VALUES (:1,:2,:3,:4,:5)",
        .{tt.name()},
    );

    defer allocator.free(q1);
    try std.testing.expectEqualStrings(insert_query, q1);

    const insert_query_2 = try tmd.insertQuery(&[_][]const u8{ "ID", "NAME" });
    defer allocator.free(insert_query_2);
    const q2 = try std.fmt.allocPrint(
        allocator,
        "INSERT INTO {s} (ID,NAME) VALUES (:1,:2)",
        .{tt.name()},
    );
    defer allocator.free(q2);
    try std.testing.expectEqualStrings(insert_query_2, q2);
}

pub fn columnCount(self: TableMetadata) usize {
    if (self.columns) |columns| return columns.len;
    return 0;
}
