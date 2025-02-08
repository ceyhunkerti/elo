const std = @import("std");
const cli = @import("cli.zig");

pub const wire = @import("wire/wire.zig");
pub const proto = @import("wire/proto/proto.zig");
pub const endpoints = @import("endpoints/endpoints.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) @panic("memory leaked");
    }
    const allocator = gpa.allocator();

    try cli.init(allocator);
}

test {
    std.testing.refAllDecls(@This());
}
