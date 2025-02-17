const Table = @This();

const std = @import("std");
const Column = @import("Column.zig");

pub const TableName = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    schema: []const u8,
    tablename: []const u8,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, default_schema: ?[]const u8) !TableName {
        var tn = TableName{
            .allocator = allocator,
            .name = undefined,
            .schema = undefined,
            .tablename = undefined,
        };

        var tokens = std.mem.split(u8, name, ".");
        const schema_or_tablename = tokens.first();
        const tablename = tokens.rest();

        if (tablename.len == 0) {
            tn.schema = try allocator.dupe(u8, default_schema orelse "");
            tn.tablename = try allocator.dupe(u8, schema_or_tablename);
            tn.name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ tn.schema, tn.tablename });
        } else {
            tn.name = try allocator.dupe(u8, name);
            tn.schema = try allocator.dupe(u8, schema_or_tablename);
            tn.tablename = try allocator.dupe(u8, tablename);
        }

        return tn;
    }

    pub fn deinit(self: TableName) void {
        self.allocator.free(self.name);
        self.allocator.free(self.schema);
        self.allocator.free(self.tablename);
    }
};
test "TableName" {
    const allocator = std.testing.allocator;
    const tn1 = try TableName.init(allocator, "DEMO.TEST_TABLE", null);
    defer tn1.deinit();

    try std.testing.expectEqualStrings("DEMO.TEST_TABLE", tn1.name);
    try std.testing.expectEqualStrings("DEMO", tn1.schema);
    try std.testing.expectEqualStrings("TEST_TABLE", tn1.tablename);

    const tn2 = try TableName.init(allocator, "TEST_TABLE", "SYS");
    defer tn2.deinit();

    try std.testing.expectEqualStrings("SYS.TEST_TABLE", tn2.name);
    try std.testing.expectEqualStrings("SYS", tn2.schema);
    try std.testing.expectEqualStrings("TEST_TABLE", tn2.tablename);
}

allocator: std.mem.Allocator = undefined,
name: TableName = undefined,
columns: []Column = undefined,

pub fn init(allocator: std.mem.Allocator, name: []const u8, columns: []Column) Table {
    return Table{
        .allocator = allocator,
        .name = TableName.init(allocator, name, null) catch unreachable,
        .columns = columns,
    };
}

pub fn deinit(self: Table) void {
    self.name.deinit();
    for (self.columns) |*column| {
        column.deinit();
    }
    self.allocator.free(self.columns);
}

pub fn insertSql(self: Table, columns: ?[]const []const u8) ![]const u8 {
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
