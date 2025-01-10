const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const commons = @import("../../commons.zig");
const Metadata = commons.Metadata;
const Field = commons.Field;
const allocPrint = std.fmt.allocPrint;
const Statement = @import("Statement.zig");
const c = @import("c.zig").c;
const t = @import("testing/testing.zig");

const Error = error{
    FailedToDpiGetQueryInfo,
};

pub const Column = struct {
    allocator: Allocator,
    index: u32,
    name: []const u8,
    nullable: bool = true,
    dpi_type_info: c.dpiDataTypeInfo,

    pub fn init(allocator: Allocator, stmt: *Statement, index: u32) !Column {
        var info: c.dpiQueryInfo = undefined;
        if (c.dpiStmt_getQueryInfo(stmt.stmt, index, &info) < 0) {
            std.debug.print("Failed to get query info with error: {s}\n", .{stmt.conn.getErrorMessage()});
            return error.FailedToDpiGetQueryInfo;
        }
        return Column{
            .allocator = allocator,
            .index = index,
            .name = allocator.dupe(u8, std.mem.span(info.name)) catch unreachable,
            .nullable = info.nullOk > 0,
            .dpi_type_info = info.typeInfo,
        };
    }
};

pub const Query = struct {
    allocator: Allocator,
    stmt: *Statement,
    columns: []Column,

    pub fn init(allocator: Allocator, stmt: *Statement) !Query {
        const md = Query{
            .allocator = allocator,
            .stmt = stmt,
            .columns = try allocator.alloc(Column, stmt.column_count),
        };

        // std.debug.print("\ncolc: {d}\n", .{stmt.column_count});

        for (md.columns, 1..) |*column, i| {
            column.* = try Column.init(allocator, stmt, @intCast(i));
        }
        return md;
    }

    pub fn columnNames(self: Query) ![]const []const u8 {
        var names = try self.allocator.alloc([]const u8, self.columns.len);
        for (self.columns, 0..) |column, i| {
            names[i] = column.name;
        }
        return names;
    }

    pub fn columnCount(self: Query) u16 {
        return @intCast(self.columns.len);
    }
};

test Query {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var conn = try t.getTestConnection(allocator);
    var stmt = try conn.prepareStatement("select 1 as A, 2 as B from dual");
    try stmt.execute();

    var query = try Query.init(allocator, &stmt);
    try testing.expectEqual(query.columnCount(), 2);
    const column_names = try query.columnNames();

    try testing.expectEqualStrings(column_names[0], "A");
    try testing.expectEqualStrings(column_names[1], "B");
}

pub const Expr = struct {
    pub const Column = struct {
        allocator: Allocator,
        field: Field,

        pub fn init(allocator: Allocator, field: Field) Expr.Column {
            return Expr.Column{
                .allocator = allocator,
                .field = field,
            };
        }

        pub fn name(self: Expr.Column) []const u8 {
            return if (self.field.name) |n| n else allocPrint(self.allocator, "col_{d}", .{self.field.index}) catch unreachable;
        }

        pub fn notNull(self: Expr.Column) []const u8 {
            return if (!self.field.nullable) "not null" else "";
        }

        pub fn defaultValue(self: Expr.Column) []const u8 {
            if (self.field.default) |d| {
                return switch (d) {
                    .String => |v| {
                        if (v) |val| {
                            return allocPrint(self.allocator, "default '{s}'", .{val}) catch unreachable;
                        }
                        return "";
                    },
                    .Double => |v| {
                        if (v) |val| {
                            return allocPrint(self.allocator, "default {d}", .{val}) catch unreachable;
                        }
                        return "";
                    },
                    .Int => |v| {
                        if (v) |val| {
                            return allocPrint(self.allocator, "default {d}", .{val}) catch unreachable;
                        }
                        return "";
                    },
                    .Boolean => |v| {
                        if (v) |val| {
                            return allocPrint(self.allocator, "default {s}", .{if (val) "1" else "0"}) catch unreachable;
                        }
                        return "";
                    },
                    .TimeStamp => "", //todo timestamp not supported
                    else => unreachable,
                };
            }
            return "";
        }

        pub fn sizing(self: Expr.Column) []const u8 {
            if (self.field.type) |tp| switch (tp) {
                .String => return allocPrint(self.allocator, "({d})", .{self.field.length orelse 255}) catch unreachable,
                .TimeStamp => return "",
                .Boolean => return "(1)",
                else => {},
            };
            if (self.field.precision) |p| if (self.field.scale) |s| {
                return allocPrint(self.allocator, "({d},{d})", .{ p, s }) catch unreachable;
            };
            if (self.field.precision) |p| {
                return allocPrint(self.allocator, "({d})", .{p}) catch unreachable;
            }
            if (self.field.scale) |s| {
                return allocPrint(self.allocator, "(*,{d})", .{s}) catch unreachable;
            }
            return "(255)";
        }

        pub fn dataType(self: Expr.Column) []const u8 {
            return if (self.field.type) |tp| switch (tp) {
                .String => "varchar",
                .Int => "number",
                .Double => "number",
                .Boolean => "number",
                .TimeStamp => "timestamp",
                else => unreachable,
            } else "varchar";
        }

        pub fn toString(self: Expr.Column) []const u8 {
            // this may have extra spaces in between the tokens
            return std.mem.trimRight(u8, allocPrint(
                self.allocator,
                "{s} {s}{s} {s} {s}",
                .{
                    self.name(),
                    self.dataType(),
                    self.sizing(),
                    self.notNull(),
                    self.defaultValue(),
                },
            ) catch unreachable, " ");
        }
    };

    allocator: Allocator,
    metadata: Metadata,

    pub fn init(allocator: Allocator, metadata: Metadata) Expr {
        return Expr{
            .allocator = allocator,
            .metadata = metadata,
        };
    }

    pub fn toString(self: Expr) []const u8 {
        const table_name = self.metadata.name;
        const columns: [][]const u8 = self.allocator.alloc([]const u8, self.metadata.fields.len) catch unreachable;
        for (self.metadata.fields, 0..) |field, i| {
            columns[i] = Expr.Column.init(self.allocator, field).toString();
        }
        return allocPrint(self.allocator, "create table {s} ({s})", .{
            table_name,
            mem.join(self.allocator, ",\n", columns) catch unreachable,
        }) catch unreachable;
    }
};

test "Expr.toString" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try testing.expectEqualStrings(
        "create table table_1 (col_1 varchar(255),\ncolumn_b varchar(255))",
        Expr.init(allocator, .{
            .name = "table_1",
            .fields = &.{
                .{
                    .index = 1,
                    .type = .String,
                    .length = 255,
                },
                .{
                    .index = 2,
                    .name = "column_b",
                    .type = .String,
                    .length = 255,
                },
            },
        }).toString(),
    );
}

test "Expr.Column.notNull" {
    try testing.expectEqualStrings(
        "",
        Expr.Column.init(testing.allocator, .{ .index = 1, .type = .Int, .nullable = true, .default = null }).notNull(),
    );
}

test "Expr.Column.defaultValue" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try testing.expectEqualStrings(
        "default 1",
        Expr.Column.init(allocator, .{
            .index = 1,
            .type = .Int,
            .nullable = false,
            .default = .{ .Int = 1 },
        }).defaultValue(),
    );
    try testing.expectEqualStrings(
        "default 'hello'",
        Expr.Column.init(allocator, .{
            .index = 1,
            .type = .String,
            .nullable = false,
            .default = .{
                .String = allocator.dupe(u8, "hello") catch unreachable,
            },
        }).defaultValue(),
    );
    try testing.expectEqualStrings("default 1", Expr.Column.init(allocator, .{
        .index = 1,
        .type = .Boolean,
        .nullable = false,
        .default = .{ .Boolean = true },
    }).defaultValue());
    try testing.expectEqualStrings("default 1.1", Expr.Column.init(allocator, .{
        .index = 1,
        .type = .Double,
        .nullable = false,
        .default = .{ .Double = 1.1 },
    }).defaultValue());
}

test "Expr.Column.sizing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try testing.expectEqualStrings("(1,2)", Expr.Column.init(allocator, .{ .index = 1, .precision = 1, .scale = 2 }).sizing());
    try testing.expectEqualStrings("(1)", Expr.Column.init(allocator, .{ .index = 1, .precision = 1, .scale = null }).sizing());
    try testing.expectEqualStrings("(*,2)", Expr.Column.init(allocator, .{ .index = 1, .precision = null, .scale = 2 }).sizing());
    try testing.expectEqualStrings("(255)", Expr.Column.init(allocator, .{ .index = 1, .precision = null, .scale = null }).sizing());
}

test "Expr.Column.dataType" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try testing.expectEqualStrings("varchar", Expr.Column.init(
        allocator,
        .{ .index = 1, .type = .String, .nullable = true, .default = null },
    ).dataType());

    try testing.expectEqualStrings("varchar", Expr.Column.init(
        allocator,
        .{ .index = 1, .type = .String, .nullable = true, .default = null, .length = 255 },
    ).dataType());

    try testing.expectEqualStrings("number", Expr.Column.init(
        allocator,
        .{ .index = 1, .type = .Int, .nullable = true, .default = null },
    ).dataType());

    try testing.expectEqualStrings("number", Expr.Column.init(
        allocator,
        .{ .index = 1, .type = .Double, .nullable = true, .default = null },
    ).dataType());

    try testing.expectEqualStrings("number", Expr.Column.init(
        allocator,
        .{ .index = 1, .type = .Boolean, .nullable = true, .default = null },
    ).dataType());

    try testing.expectEqualStrings("timestamp", Expr.Column.init(
        allocator,
        .{ .index = 1, .type = .TimeStamp, .nullable = true, .default = null },
    ).dataType());
}

test "Expr.Column.toString" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try testing.expectEqualStrings(
        "col_1 varchar(255)",
        Expr.Column.init(allocator, .{ .index = 1, .type = .String, .length = 255 }).toString(),
    );

    try testing.expectEqualStrings(
        "col_1 varchar(255) not null",
        Expr.Column.init(allocator, .{ .index = 1, .nullable = false }).toString(),
    );
}
