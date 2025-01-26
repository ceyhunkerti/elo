const std = @import("std");

const Context = @import("Context.zig");

pub fn check(result: c_int, err: anyerror, ctx: ?*const Context) !void {
    if (result < 0) {
        if (ctx) |c| c.printError();
        return err;
    }
}
