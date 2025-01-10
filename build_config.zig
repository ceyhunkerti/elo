// const std = @import("std");
// const Step = std.Build.Step;

// pub const BuildConfig = struct {
//     build: *std.Build,
//     version: std.SemanticVersion,

//     use_oracle: bool = false,

//     pub fn init(b: *std.Build) !BuildConfig {
//         return .{
//             .build = b,
//             .version = .{ .major = 0, .minor = 0, .patch = 0 },
//             .use_oracle = b.option(bool, "oracle", "use oracle endpoints") orelse false,
//         };
//     }

//     pub fn configure(self: BuildConfig, step: *Step.Compile) void {
//         const options = self.build.addOptions();
//         options.addOption(bool, "use_oracle", self.use_oracle);
//         step.root_module.addOptions("config", options);

//         if (self.use_oracle) {
//             std.debug.print("Step {s}: Using oracle endpoints.\n", .{step.name});
//             step.addCSourceFile(.{
//                 .file = self.build.path("lib/oracle/odpi-5.4.1/embed/dpi.c"),
//             });
//             step.addIncludePath(self.build.path("lib/oracle/odpi-5.4.1/include"));
//         }
//     }
// };
