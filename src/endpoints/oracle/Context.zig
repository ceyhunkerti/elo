const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @import("c.zig").c;
pub const Self = @This();

pub const ContextError = error{
    FailedToCreateContext,
};

dpi_context: ?*c.dpiContext = undefined,

pub fn create(self: *Self) !void {
    try self.createWithParams();
}

fn createWithParams(self: *Self) !void {
    var err: c.dpiErrorInfo = undefined;
    if (c.dpiContext_createWithParams(c.DPI_MAJOR_VERSION, c.DPI_MINOR_VERSION, null, &self.dpi_context, &err) < 0) {
        std.debug.print("Failed to create context with error: {s}\n", .{err.message});
        return error.FailedToCreateContext;
    }
}

pub fn errorMessage(self: *const Self) []const u8 {
    var err: c.dpiErrorInfo = undefined;
    c.dpiContext_getError(self.dpi_context, &err);
    return std.mem.span(err.message);
}

pub fn printError(self: *const Self) void {
    var err: c.dpiErrorInfo = undefined;
    c.dpiContext_getError(self.dpi_context, &err);
    std.debug.print("Error: {d} {s}\n", .{ err.code, err.message });
}
