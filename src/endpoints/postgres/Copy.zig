const Copy = @This();
const std = @import("std");

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
table: []const u8,
columns: ?[]const []const u8 = null,
with_options: ?Options,

pub fn init(
    allocator: std.mem.Allocator,
    table: []const u8,
    columns: ?[]const []const u8,
    with_options: ?Options,
) Copy {
    return Copy{
        .allocator = allocator,
        .table = table,
        .columns = columns,
        .with_options = with_options,
    };
}

pub fn toString(self: Copy) ![]u8 {
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
    defer if (columns) |c| self.allocator.free(c);

    if (columns) |c| {
        try list.append('(');
        try list.appendSlice(c);
        try list.append(')');
    }
    try list.appendSlice(" FROM STDIN");

    const options: ?[]const u8 = brk: {
        if (self.with_options) |with| {
            break :brk try with.toString(self.allocator);
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
