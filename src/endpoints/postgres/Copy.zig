const Copy = @This();

const std = @import("std");
const c = @import("c.zig").c;
const Connection = @import("Connection.zig");

pub const Options = struct {
    format: ?[]const u8 = null,
    freeze: ?[]const u8 = null,
    delimiter: ?[]const u8 = null,
    null: ?[]const u8 = null,
    default: ?[]const u8 = null,
    header: ?[]const u8 = null,
    quote: ?[]const u8 = null,
    escape: ?[]const u8 = null,
    force_quote: ?[]const u8 = null,
    force_not_null: ?[]const u8 = null,
    force_null: ?[]const u8 = null,
    on_error: ?[]const u8 = null,
    encoding: ?[]const u8 = null,
    log_verbosity: ?[]const u8 = null,

    pub fn toString(self: Options, allocator: std.mem.Allocator) ![]u8 {
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();
        const fields = std.meta.fields(@TypeOf(self));

        inline for (fields) |field| {
            const name = field.name;
            const value = @field(self, field.name);

            if (value) |v| {
                if (list.items.len > 0) {
                    try list.appendSlice(", ");
                }
                try list.writer().print("{s} {s}", .{ name, v });
            }
        }

        return try list.toOwnedSlice();
    }
};

allocator: std.mem.Allocator,
conn: *Connection,
table: []const u8,
columns: ?[]const []const u8 = null,
options: ?Options,

pub fn init(
    allocator: std.mem.Allocator,
    conn: *Connection,
    table: []const u8,
    columns: ?[]const []const u8,
    options: ?Options,
) Copy {
    return Copy{
        .allocator = allocator,
        .conn = conn,
        .table = table,
        .columns = columns,
        .options = options,
    };
}

pub fn command(self: Copy) ![]u8 {
    var list = std.ArrayList(u8).init(self.allocator);
    defer list.deinit();

    try list.appendSlice("COPY ");
    try list.appendSlice(self.table);
    const columns: ?[]const u8 = brk: {
        if (self.columns) |columns| {
            break :brk try std.mem.join(self.allocator, ",", columns);
        } else {
            break :brk null;
        }
    };
    defer if (columns) |col| self.allocator.free(col);

    if (columns) |col| {
        try list.append('(');
        try list.appendSlice(col);
        try list.append(')');
    }
    try list.appendSlice(" FROM STDIN");

    const options: ?[]const u8 = brk: {
        if (self.options) |o| {
            break :brk try o.toString(self.allocator);
        } else {
            break :brk null;
        }
    };
    defer if (options) |o| self.allocator.free(o);

    if (options) |o| {
        try list.appendSlice(" WITH (");
        try list.appendSlice(o);
        try list.append(')');
    }

    try list.appendSlice("\x00");
    return try list.toOwnedSlice();
}

pub fn execute(self: Copy) !void {
    const copy_command = try self.command();
    defer self.allocator.free(copy_command);
    const res = c.PQexec(self.conn.pg_conn, @ptrCast(copy_command.ptr));
    if (c.PQresultStatus(res) != c.PGRES_COPY_IN) {
        std.debug.print("Error executing COPY: {s}\n", .{std.mem.span(c.PQresultErrorMessage(res))});
        return error.Fail;
    }
    c.PQclear(res);
}

pub fn end(self: Copy) !void {
    if (c.PQputCopyEnd(self.conn.pg_conn, null) != 1) {
        std.debug.print("Failed to send COPY end signal\n", .{});
        return error.Fail;
    }
}

pub fn copy(self: Copy, data: []const u8) !void {
    if (c.PQputCopyData(self.conn.pg_conn, @ptrCast(data.ptr), @intCast(data.len)) != 1) {
        std.debug.print("Failed to send COPY data: {s}\n", .{self.conn.errorMessage()});
        return error.Fail;
    }
}
