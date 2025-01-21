const std = @import("std");
const c = @import("c.zig").c;

pub const OciError = error{
    OciFailure,
    OciSuccessWithInfo,
    Fatal,
    PrintErrorFailed,
    InvalidHandle,
};

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

pub fn check(error_handle: ?*c.OCIError, return_code: i32) !void {
    var error_code: i32 = 0;

    switch (return_code) {
        c.OCI_SUCCESS => {},
        c.OCI_SUCCESS_WITH_INFO => {
            std.debug.print("Error - OCI_SUCCESS_WITH_INFO\n", .{});
        },
        c.OCI_NEED_DATA => {
            std.debug.print("Error - OCI_NEED_DATA\n", .{});
        },
        c.OCI_NO_DATA => {
            std.debug.print("Error - OCI_NODATA\n", .{});
        },
        c.OCI_ERROR => {
            if (error_handle) |eh| {
                try printError(eh, c.OCI_HTYPE_ERROR, &error_code);
            } else {
                std.debug.print("Error - OCI_ERROR\n", .{});
            }
            return error.OciFailure;
        },
        c.OCI_INVALID_HANDLE => {
            std.debug.print("Error - OCI_INVALID_HANDLE\n", .{});
            return error.InvalidHandle;
        },
        c.OCI_STILL_EXECUTING => {
            std.debug.print("Error - OCI_STILL_EXECUTING\n", .{});
        },
        c.OCI_CONTINUE => {
            std.debug.print("Error - OCI_CONTINUE\n", .{});
        },
        else => {
            std.debug.print("Unknown return code", .{});
        },
    }
}
