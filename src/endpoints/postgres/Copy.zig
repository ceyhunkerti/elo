const Copy = @This();

const std = @import("std");
const c = @import("c.zig").c;
const b = @import("base");
const Connection = @import("Connection.zig");

pub const Options = struct {
    format: []const u8,
    delimiter: []const u8,
    freeze: ?[]const u8 = null,
    null: ?[]const u8 = null,
    default: ?[]const u8 = null,
    header: ?[]const u8 = null,
    quote: ?[]const u8 = null,
    escape: ?[]const u8 = null,

    pub fn toString(self: Options, allocator: std.mem.Allocator) ![]u8 {
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();
        const fields = std.meta.fields(@TypeOf(self));

        inline for (fields) |field| {
            const name = field.name;
            const value = @field(self, field.name);

            const is_optional = @typeInfo(field.type) == .Optional;
            if (is_optional) {
                if (value) |v| {
                    if (list.items.len > 0) {
                        try list.appendSlice(", ");
                    }
                    try list.writer().print("{s} {s}", .{ name, v });
                }
            } else {
                if (list.items.len > 0) {
                    try list.appendSlice(", ");
                }
                try list.writer().print("{s} {s}", .{ name, value });
            }
        }

        return try list.toOwnedSlice();
    }

    pub fn fromMap(allocator: std.mem.Allocator, map: std.StringHashMap([]const u8)) !Options {
        const format = map.get("copy-format") orelse "CSV";
        const delimiter = map.get("copy-delimiter") orelse "','";

        const freeze = map.get("copy-freeze");
        const copy_null = map.get("copy-null");
        const default = map.get("copy-default");
        const header = map.get("copy-header");
        const quote = map.get("copy-quote");
        const escape = map.get("copy-escape");

        return .{
            .format = try allocator.dupe(u8, format),
            .delimiter = try allocator.dupe(u8, delimiter),

            .freeze = if (freeze) |f| try allocator.dupe(u8, f) else null,
            .null = if (copy_null) |n| try allocator.dupe(u8, n) else null,
            .default = if (default) |d| try allocator.dupe(u8, d) else null,
            .header = if (header) |h| try allocator.dupe(u8, h) else null,
            .quote = if (quote) |q| try allocator.dupe(u8, q) else null,
            .escape = if (escape) |e| try allocator.dupe(u8, e) else null,
        };
    }

    pub fn deinit(self: Options, allocator: std.mem.Allocator) void {
        allocator.free(self.format);
        allocator.free(self.delimiter);

        if (self.freeze) |f| allocator.free(f);
        if (self.null) |n| allocator.free(n);
        if (self.default) |d| allocator.free(d);
        if (self.header) |h| allocator.free(h);
        if (self.quote) |q| allocator.free(q);
        if (self.escape) |e| allocator.free(e);
    }

    pub fn help(output: *std.ArrayList(u8)) !void {
        try output.appendSlice(
            \\Copy command options:
            \\
            \\--copy-format [OPTIONAL]
            \\
            \\--copy-freeze [OPTIONAL]
            \\
            \\--copy-delimiter [OPTIONAL]
            \\
            \\--copy-null [OPTIONAL]
            \\
            \\--copy-default [OPTIONAL]
            \\
            \\--copy-header [OPTIONAL]
            \\
            \\--copy-quote [OPTIONAL]
            \\
            \\--copy-escape [OPTIONAL]
            \\
        );
    }
};

allocator: std.mem.Allocator,
conn: *Connection,
table: []const u8,
columns: ?[]const []const u8 = null,
options: ?Options,

batch_size: u32 = 10_000,
batch_index: u32 = 0,

data: std.ArrayList(u8),

pub fn init(
    allocator: std.mem.Allocator,
    conn: *Connection,
    table: []const u8,
    columns: ?[]const []const u8,
    options: ?Options,
    batch_size: u32,
) Copy {
    return Copy{
        .allocator = allocator,
        .conn = conn,
        .table = table,
        .columns = columns,
        .options = options,
        .batch_size = batch_size,
        .data = std.ArrayList(u8).initCapacity(allocator, batch_size * 1_000) catch unreachable, // 1_000 arbitrary record size
    };
}

pub fn deinit(self: Copy) void {
    self.data.deinit();
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
    } else {
        try list.appendSlice(" WITH (FORMAT CSV delimiter ',')");
    }

    try list.appendSlice("\x00");
    return try list.toOwnedSlice();
}

pub fn start(self: Copy) !void {
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

    const result = c.PQgetResult(self.conn.pg_conn);
    defer if (result != null) c.PQclear(result);

    if (result == null) {
        return error.Fail;
    }

    const status = c.PQresultStatus(result);
    if (status != c.PGRES_COMMAND_OK) {
        const err_msg = c.PQresultErrorMessage(result);
        std.debug.print("COPY failed: {s}\n", .{err_msg});
        return error.Fail;
    }
}

pub fn flush(self: Copy) !void {
    if (c.PQputCopyData(self.conn.pg_conn, @ptrCast(self.data.items.ptr), @intCast(self.data.items.len)) != 1) {
        std.debug.print("Failed to send COPY data: {s}\n", .{self.conn.errorMessage()});
        return error.Fail;
    }
}

pub fn copy(self: *Copy, record: *b.Record, formatter: b.RecordFormatter) !void {
    if (self.batch_index > 0) {
        try self.data.appendSlice("\n");
    }
    try record.write(&self.data, formatter);

    self.batch_index += 1;

    if (self.batch_index >= self.batch_size) {
        try self.flush();
        self.batch_index = 0;
        self.data.clearRetainingCapacity();
        try self.data.appendSlice("\n");
    }
}

pub fn isDirty(self: Copy) bool {
    return self.data.items.len > 0;
}

fn flushIfDirty(self: Copy) !void {
    if (self.isDirty()) try self.flush();
}

pub fn finish(self: Copy) !void {
    try self.flushIfDirty();
    try self.end();
}
