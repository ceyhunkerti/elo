const std = @import("std");
const c = @import("c.zig").c;

pub const OciError = error{ OciFailure, OciSuccessWithInfo, Fatal, PrintErrorFailed };

pub fn printError(error_handle: ?*c.OCIError, handle_type: u32, error_code: ?*i32) !void {
    if (error_handle) |eh| {
        var error_buffer: [512:0]u8 = std.mem.zeroes([512:0]u8);
        if (c.OCIErrorGet(eh, 1, null, error_code, &error_buffer, @sizeOf(@TypeOf(error_buffer)), handle_type) != c.OCI_SUCCESS) {
            std.debug.print("OCIErrorGet failed.\n", .{});
            return error.PrintErrorFailed;
        }
        std.debug.print("Error - {s}\n", .{error_buffer});
    }
}
