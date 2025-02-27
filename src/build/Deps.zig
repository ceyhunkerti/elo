const Deps = @This();

const std = @import("std");
const Step = std.Build.Step;

b: *std.Build = undefined,

pub fn init(b: *std.Build) Deps {
    return .{
        .b = b,
    };
}

pub fn setupOracle(self: Deps, step: *Step.Compile) void {
    step.addCSourceFile(.{
        .file = self.b.path("lib/oracle/odpi/embed/dpi.c"),
    });
    step.addIncludePath(self.b.path("lib/oracle/odpi/include"));
}

pub fn setupPostgres(_: Deps, step: *Step.Compile) void {
    step.linkSystemLibrary("libpq");
    step.addIncludePath(.{ .cwd_relative = "/usr/include/postgresql" });
}
