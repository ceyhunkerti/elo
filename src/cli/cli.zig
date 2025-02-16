const std = @import("std");
const argz = @import("argz");
const Command = argz.Command;
const Allocator = std.mem.Allocator;
const initListCommand = @import("list_cmd.zig").init;
const initInfoCommand = @import("info_cmd.zig").init;
const initRunCommand = @import("run_cmd.zig").init;
const Context = @import("context.zig").Context;

pub fn init(allocator: Allocator) !*Command {
    var root = Command.init(allocator, "elo", struct {
        fn run(cmd: *const Command, ctx: ?*anyopaque) anyerror!i32 {
            if (ctx) |ctx_| {
                const context: *Context = @ptrCast(@alignCast(ctx_));
                const o = try cmd.getOption("log-level");
                if (o.getString()) |level| {
                    context.log_level.* = std.meta.stringToEnum(std.log.Level, level) orelse level: {
                        std.log.warn("Invalid log level: {s}. Defaulting to .info", .{level});
                        break :level .info;
                    };
                }
            }
            return 0;
        }
    }.run);
    root.description = "Data transfer utility";
    try root.addOption(try argz.Option.init(
        allocator,
        .String,
        &.{ "l", "log-level" },
        "log level",
    ));

    const list = try initListCommand(allocator);
    const info = try initInfoCommand(allocator);
    const run = try initRunCommand(allocator);

    try root.addCommand(list);
    try root.addCommand(info);
    try root.addCommand(run);

    return root;
}
