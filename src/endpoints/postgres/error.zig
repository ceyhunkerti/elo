const std = @import("std");
const c = @import("c.zig").c;

pub fn resultError(res: c.PGresult) []const u8 {
    const s = c.PQresultErrorMessage(res);
    return std.mem.span(s);
}
