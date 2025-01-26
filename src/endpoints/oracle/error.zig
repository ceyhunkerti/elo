const std = @import("std");

const Context = @import("Context.zig");

pub inline fn check(result: c_int, err: anyerror) !void {
    if (result < 0) return err;
}
