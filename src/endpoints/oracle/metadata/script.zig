const std = @import("std");
const Allocator = std.mem.Allocator;
const commons = @import("../../../commons.zig");
const Field = commons.Field;
const FieldValue = commons.FieldValue;
const Metadata = commons.Metadata;
const allocPrint = std.fmt.allocPrint;
const testing = std.testing;

pub const ColumnScript = struct {
    pub fn fromField(allocator: Allocator, field: Field) ![]const u8 {
        const name = if (field.name) |n| n else try allocPrint(allocator, "col_{d}", .{field.index});
        const not_null = if (!field.nullable) "not null" else "";
        const default_value = brk: {
            if (field.default) |d| {
                switch (d) {
                    .String => |v| {
                        if (v) |val| {
                            break :brk try allocPrint(allocator, "default '{s}'", .{val});
                        }
                        break :brk "";
                    },
                    .Double => |v| {
                        if (v) |val| {
                            break :brk try allocPrint(allocator, "default {d}", .{val});
                        }
                        break :brk "";
                    },
                    .Int => |v| {
                        if (v) |val| {
                            break :brk try allocPrint(allocator, "default {d}", .{val});
                        }
                        break :brk "";
                    },
                    .Boolean => |v| {
                        if (v) |val| {
                            break :brk try allocPrint(allocator, "default {s}", .{if (val) "1" else "0"});
                        }
                        break :brk "";
                    },
                    .TimeStamp => break :brk "", //todo timestamp not supported
                    else => unreachable,
                }
            }
            break :brk "";
        };
        const sizing = brk: {
            if (field.type) |tp| switch (tp) {
                .String => break :brk try allocPrint(allocator, "({d})", .{field.length orelse 255}),
                .TimeStamp => break :brk "",
                .Boolean => break :brk "(1)",
                else => break :brk "",
            };
            if (field.precision) |p| if (field.scale) |s| {
                break :brk try allocPrint(allocator, "({d},{d})", .{ p, s });
            };
            if (field.precision) |p| {
                break :brk try allocPrint(allocator, "({d})", .{p});
            }
            if (field.scale) |s| {
                break :brk try allocPrint(allocator, "(*,{d})", .{s});
            }
            break :brk "(255)";
        };
        const data_type = if (field.type) |tp| switch (tp) {
            .String => "varchar",
            .Int => "number",
            .Double => "number",
            .Boolean => "number",
            .TimeStamp => "timestamp",
            else => unreachable,
        };

        return std.mem.trimRight(u8, allocPrint(
            allocator,
            "{s} {s}{s} {s} {s}",
            .{
                name,
                data_type,
                sizing,
                not_null,
                default_value,
            },
        ) catch unreachable, " ");
    }
};

pub const CreateTableScript = struct {
    pub fn fromMetadata(allocator: Allocator, metadata: Metadata) ![]const u8 {
        const table_name = metadata.name;
        const columns: [][]const u8 = allocator.alloc([]const u8, metadata.fields.len) catch unreachable;
        for (metadata.fields, 0..) |field, i| {
            columns[i] = try ColumnScript.fromField(allocator, field);
        }
        return allocPrint(allocator, "create table {s} ({s})", .{
            table_name,
            std.mem.join(allocator, ",\n", columns) catch unreachable,
        }) catch unreachable;
    }
};

test CreateTableScript {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try testing.expectEqualStrings(
        "create table table_1 (col_1 varchar(255),\ncolumn_b varchar(255))",
        try CreateTableScript.fromMetadata(allocator, .{
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
        }),
    );
}
