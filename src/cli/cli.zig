const std = @import("std");
const argz = @import("argz");
const Command = argz.Command;
const Allocator = std.mem.Allocator;
const initListCommand = @import("list_cmd.zig").init;
const initInfoCommand = @import("info_cmd.zig").init;
const initRunCommand = @import("run_cmd.zig").init;

pub fn init(allocator: Allocator) !*Command {
    var root = Command.init(allocator, "elo", null);
    root.description = "Data transfer utility";

    const list = try initListCommand(allocator);
    const info = try initInfoCommand(allocator);
    const run = try initRunCommand(allocator);

    try root.addCommand(list);
    try root.addCommand(info);
    try root.addCommand(run);

    return root;
}
