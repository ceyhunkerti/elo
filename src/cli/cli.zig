const std = @import("std");
const argz = @import("argz");
const Command = argz.Command;
const initListCommand = @import("list_cmd.zig").init;
const initInfoCommand = @import("info_cmd.zig").init;
const initRunCommand = @import("run_cmd.zig").init;

pub fn init(allocator: std.mem.Allocator) !Command {
    var root = Command.init(allocator, "elo", null);
    var list = try initListCommand(allocator);
    try root.addCommand(&list);
    var info = try initInfoCommand(allocator);
    try root.addCommand(&info);
    var run = try initRunCommand(allocator);
    try root.addCommand(&run);

    return root;
}
