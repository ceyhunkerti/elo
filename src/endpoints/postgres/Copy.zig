const Copy = @This();
const std = @import("std");

pub const Options = struct {
    format: ?[]const u8,
    freeze: ?bool,
    delimiter: ?[]const u8,
    null: ?[]const u8,
    default: ?[]const u8,
    header: ?[]const u8,
    quote: ?[]const u8,
    escape: ?[]const u8,
    force_quote: ?[]const u8,
    force_not_null: ?[]const u8,
    force_null: ?[]const u8,
    on_error: ?[]const u8,
    encoding: ?[]const u8,
    log_verbosity: ?[]const u8,

    pub fn toString(self: Options, allocator: std.mem.Allocator) ![:0]u8 {
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

        return list.toOwnedSlice();
    }
};

allocator: std.mem.Allocator,
table: []const u8,
columns: ?[]const []const u8 = null,
from: []const u8,
with_options: ?Options,

pub fn init(
    allocator: std.mem.Allocator,
    table: []const u8,
    columns: ?[]const []const u8,
    from: []const u8,
    with_options: ?Options,
) Copy {
    return Copy{
        .allocator = allocator,
        .table = table,
        .columns = columns,
        .from = from,
        .with_options = with_options,
    };
}

pub fn toString(self: Copy, allocator: std.mem.Allocator) ![]u8 {
    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();

    try list.appendSlice("COPY ");
    try list.appendSlice(self.table);
    const columns: ?[]const u8 = brk: {
        if (self.columns) |columns| {
            break :brk try std.mem.join(allocator, ",", columns);
        } else {
            break :brk null;
        }
    };
    defer if (columns) |c| allocator.free(c);

    if (columns) |c| {
        try list.append('(');
        try list.appendSlice(c);
        try list.append(')');
    }
    try list.appendSlice(" FROM ");
    try list.appendSlice(self.from);

    const options: ?[]const u8 = brk: {
        if (self.with) |with| {
            break :brk try with.toString(allocator);
        } else {
            break :brk null;
        }
    };
    defer if (options) |o| allocator.free(o);

    if (options) |o| {
        try list.appendSlice(" WITH (");
        try list.appendSlice(o);
        try list.append(')');
    }

    return list.toOwnedSlice();
}
