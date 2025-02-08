const std = @import("std");
const argz = @import("argz");

pub fn init(allocator: std.mem.Allocator) !void {
    var root = argz.Command.init(allocator, "elo", null);
    defer root.deinit();
    var list = argz.Command.init(allocator, "list", null);
    var endpoints = argz.Command.init(allocator, "endpoints", null);

    try root.addCommand(&list);
    try list.addCommand(&endpoints);
}
