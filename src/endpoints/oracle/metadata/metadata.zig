const c = @import("../c.zig").c;
const std = @import("std");
const Connection = @import("../Connection.zig");

pub const md = @import("../../shared/db/metadata/metadata.zig");

pub const ColumnTypeInfo = struct {
    dpi_oracle_type_num: c.dpiOracleTypeNum,
    dpi_native_type_num: c.dpiNativeTypeNum,

    pub fn init(name: []const u8) ColumnTypeInfo {
        if (std.mem.eql(u8, name, "NUMBER")) {
            return .{
                .dpi_oracle_type_num = c.DPI_ORACLE_TYPE_NUMBER,
                .dpi_native_type_num = c.DPI_NATIVE_TYPE_DOUBLE,
            };
        } else if (std.mem.eql(u8, name, "VARCHAR2")) {
            return .{
                .dpi_oracle_type_num = c.DPI_ORACLE_TYPE_LONG_VARCHAR,
                .dpi_native_type_num = c.DPI_NATIVE_TYPE_BYTES,
            };
        } else if (std.mem.eql(u8, name, "CHAR")) {
            return .{
                .dpi_oracle_type_num = c.DPI_ORACLE_TYPE_CHAR,
                .dpi_native_type_num = c.DPI_NATIVE_TYPE_BYTES,
            };
        } else if (std.mem.eql(u8, name, "DATE") or std.mem.eql(u8, name, "TIMESTAMP")) {
            return .{
                .dpi_oracle_type_num = c.DPI_ORACLE_TYPE_TIMESTAMP,
                .dpi_native_type_num = c.DPI_NATIVE_TYPE_TIMESTAMP,
            };
        } else {
            // todo
            std.debug.print("Unsupported type: {s}\n", .{name});
            unreachable;
        }
    }
};
pub const Column = md.Column(ColumnTypeInfo);
pub const TableName = md.TableName;
pub const Table = md.Table(ColumnTypeInfo);

pub fn getTableMetadata(allocator: std.mem.Allocator, conn: *Connection, table_name: []const u8) !Table {
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
        .{ table.tablename, table.schema },
    );
    std.debug.print("{s}\n", .{sql});
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
        var map = try record.asMap(allocator, column_names);
        defer map.deinit();
        defer record.deinit(allocator);

        const index = map.get("column_id").?.Double.?;
        const name = map.get("column_name").?.Bytes.?;
        const size = map.get("data_length").?.Double;
        const nullable = map.get("nullable").?.Bytes.?[0] == 'Y';
        const type_name = map.get("data_type").?.Bytes.?;

        const column = Column.init(
            allocator,
            std.math.lossyCast(u32, index),
            try allocator.dupe(u8, name),
            .{
                .size = if (size) |sz| std.math.lossyCast(u32, sz) else null,
                .precision = null,
                .nullable = nullable,
                .ext = ColumnTypeInfo.init(type_name),
            },
        );
        try columns.append(column);
    }

    return .{
        .allocator = allocator,
        .name = table,
        .columns = try columns.toOwnedSlice(),
    };
}
