const ArrayBind = @This();
const std = @import("std");

const Statement = @import("Statement.zig");
const Connection = @import("Connection.zig");
const md = @import("metadata/metadata.zig");
const c = @import("c.zig").c;
const e = @import("error.zig");
const b = @import("base");

allocator: std.mem.Allocator = undefined,
stmt: *Statement = undefined,
columns: []md.Column = undefined,
capacity: u32 = undefined,

dpi_var_array: ?[]?*c.dpiVar = null,
dpi_data_array: ?[]?[*c]c.dpiData = null,

pub fn init(allocator: std.mem.Allocator, stmt: *Statement, columns: []md.Column, capacity: u32) !ArrayBind {
    var dpi_var_array: ?[]?*c.dpiVar = try allocator.alloc(?*c.dpiVar, columns.len);
    var dpi_data_array: ?[]?[*c]c.dpiData = try allocator.alloc(?[*c]c.dpiData, columns.len);

    for (columns, 0..) |column, ci| {
        const size = switch (column.type_info.?.ext.?.dpi_native_type_num) {
            c.DPI_NATIVE_TYPE_BYTES => column.type_info.?.size orelse unreachable,
            c.DPI_NATIVE_TYPE_INT64 => 0,
            else => 0,
        };

        try stmt.conn.newDpiVariable(
            column.type_info.?.ext.?.dpi_oracle_type_num,
            column.type_info.?.ext.?.dpi_native_type_num,
            @intCast(capacity),
            size,
            false, // todo size_is_bytes
            false, // todo is_array
            null,
            &dpi_var_array.?[ci],
            &dpi_data_array.?[ci].?,
        );

        try e.check(c.dpiStmt_bindByPos(
            stmt.dpi_stmt,
            @as(u32, @intCast(ci)) + 1,
            dpi_var_array.?[ci].?,
        ), error.Fail);
    }

    return .{
        .allocator = allocator,
        .stmt = stmt,
        .columns = columns,
        .capacity = capacity,
        .dpi_var_array = dpi_var_array,
        .dpi_data_array = dpi_data_array,
    };
}

pub fn deinit(self: *ArrayBind) void {
    if (self.dpi_var_array) |arr| for (arr) |var_| {
        if (var_) |v| {
            if (c.dpiVar_release(v) > 0) {
                std.debug.print("Failed to release variable with error: {s}\n", .{self.stmt.conn.errorMessage()});
                unreachable;
            }
        }
    };
    if (self.dpi_var_array) |arr| self.allocator.free(arr);
    if (self.dpi_data_array) |arr| self.allocator.free(arr);
}

pub fn add(self: *ArrayBind, index: u32, record: *b.Record) !void {
    for (record.items(), 0..) |column, ci| {
        switch (column) {
            .Int => |val| {
                if (val) |v| {
                    switch (self.columns[ci].type_info.?.ext.?.dpi_native_type_num) {
                        c.DPI_NATIVE_TYPE_INT64 => {
                            self.dpi_data_array.?[ci].?[index].value.asInt64 = v;
                        },
                        c.DPI_NATIVE_TYPE_DOUBLE => {
                            self.dpi_data_array.?[ci].?[index].value.asDouble = @floatFromInt(v);
                        },
                        c.DPI_NATIVE_TYPE_FLOAT => {
                            self.dpi_data_array.?[ci].?[index].value.asFloat = @floatFromInt(v);
                        },
                        c.DPI_NATIVE_TYPE_BYTES => {
                            const str = try std.fmt.allocPrint(self.allocator, "{d}", .{v});
                            defer self.allocator.free(str);
                            if (c.dpiVar_setFromBytes(self.dpi_var_array.?[ci].?, @intCast(index), str.ptr, @intCast(str.len)) < 0) {
                                std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.stmt.conn.errorMessage()});
                                unreachable;
                            }
                        },
                        else => {
                            std.debug.print(
                                \\Type conversion from int to oracle native type num {d} is not supported
                                \\Column name: {s}
                                \\Column oracle type num: {d}
                            , .{
                                self.columns[ci].type_info.?.ext.?.dpi_native_type_num,
                                self.columns[ci].name,
                                self.columns[ci].type_info.?.ext.?.dpi_oracle_type_num,
                            });
                            return error.Fail;
                        },
                    }
                    self.dpi_data_array.?[ci].?[index].isNull = 0;
                } else {
                    self.dpi_data_array.?[ci].?[index].isNull = 1;
                }
            },
            .Double => |val| {
                if (val) |v| {
                    switch (self.columns[ci].type_info.?.ext.?.dpi_native_type_num) {
                        c.DPI_NATIVE_TYPE_INT64 => {
                            self.dpi_data_array.?[ci].?[index].value.asInt64 = @intFromFloat(v);
                        },
                        c.DPI_NATIVE_TYPE_DOUBLE => {
                            self.dpi_data_array.?[ci].?[index].value.asDouble = v;
                        },
                        c.DPI_NATIVE_TYPE_BYTES => {
                            const str = try std.fmt.allocPrint(self.allocator, "{d}", .{v});
                            defer self.allocator.free(str);
                            if (c.dpiVar_setFromBytes(self.dpi_var_array.?[ci].?, @intCast(index), str.ptr, @intCast(str.len)) < 0) {
                                std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.stmt.conn.errorMessage()});
                                unreachable;
                            }
                        },
                        else => {
                            std.debug.print(
                                \\Type conversion from int to oracle native type num {d} is not supported
                                \\Column name: {s}
                                \\Column oracle type num: {d}
                            , .{
                                self.columns[ci].type_info.?.ext.?.dpi_native_type_num,
                                self.columns[ci].name,
                                self.columns[ci].type_info.?.ext.?.dpi_oracle_type_num,
                            });
                            return error.Fail;
                        },
                    }
                    self.dpi_data_array.?[ci].?[index].isNull = 0;
                } else {
                    self.dpi_data_array.?[ci].?[index].isNull = 1;
                }
            },
            .Bytes => |val| {
                if (val) |v| {
                    if (self.columns[ci].type_info.?.ext.?.dpi_native_type_num != c.DPI_NATIVE_TYPE_BYTES) {
                        std.debug.print(
                            \\Type conversion from string to oracle native type num {d} is not supported
                            \\Column name: {s}
                            \\Column oracle type num: {d}
                        , .{
                            self.columns[ci].type_info.?.ext.?.dpi_native_type_num,
                            self.columns[ci].name,
                            self.columns[ci].type_info.?.ext.?.dpi_oracle_type_num,
                        });
                        return error.Fail;
                    }
                    if (c.dpiVar_setFromBytes(self.dpi_var_array.?[ci].?, @intCast(index), v.ptr, @intCast(v.len)) < 0) {
                        std.debug.print("Failed to setFromBytes with error: {s}\n", .{self.stmt.conn.errorMessage()});
                        unreachable;
                    }
                } else {
                    self.dpi_data_array.?[ci].?[index].isNull = 1;
                }
            },
            .Boolean => |val| {
                if (val) |v| {
                    self.dpi_data_array.?[ci].?[index].isNull = 0;
                    self.dpi_data_array.?[ci].?[index].value.asBoolean = if (v) 1 else 0;
                } else {
                    self.dpi_data_array.?[ci].?[index].isNull = 1;
                }
            },
            .TimeStamp => |val| {
                if (val) |v| {
                    self.dpi_data_array.?[ci].?[index].isNull = 0;
                    self.dpi_data_array.?[ci].?[index].value.asTimestamp.year = @intCast(v.year);
                    self.dpi_data_array.?[ci].?[index].value.asTimestamp.month = v.month;
                    self.dpi_data_array.?[ci].?[index].value.asTimestamp.day = v.day;
                    self.dpi_data_array.?[ci].?[index].value.asTimestamp.hour = v.hour;
                    self.dpi_data_array.?[ci].?[index].value.asTimestamp.minute = v.minute;
                    self.dpi_data_array.?[ci].?[index].value.asTimestamp.second = v.second;
                } else {
                    self.dpi_data_array.?[ci].?[index].isNull = 1;
                }
            },
            // todo
            else => {
                std.debug.print("\nUnhandled column in writer {any}\n", .{column});
                unreachable;
            },
        }
    }
}
