const c = @import("c.zig").c;
const std = @import("std");
const Allocator = std.mem.Allocator;
const Self = @This();
const Context = @import("Context.zig");

pub const Privilege = enum {
    SYSDBA,
    SYSOPER,
    SYSASM,
    SYSBACKUP,
    SYSDG,
    SYSKM,
    SYSRAC,

    pub fn toDpi(self: Privilege) c_int {
        return switch (self) {
            .SYSDBA => return c.OCI_SYSDBA,
            .SYSOPER => return c.OCI_SYSOPER,
            .SYSASM => return c.OCI_SYSASM,
            .SYSBACKUP => return c.OCI_SYSBKP,
            .SYSDG => return c.OCI_SYSDGD,
            .SYSKM => return c.OCI_SYSKMT,
            .SYSRAC => return c.OCI_SYSRAC,
        };
    }
};

test "Privilege.toDpi" {
    const sysdba = Privilege.SYSDBA;
    const dpi_mode = sysdba.toDpi();
    try std.testing.expect(dpi_mode == c.OCI_SYSDBA);
}

allocator: Allocator,
username: []const u8,
password: []const u8,
connect_string: []const u8,
privilege: ?Privilege = null,

app_context: std.ArrayList([3][]const u8) = undefined,

fn connectInternal(ctx: Context) !void {
    &ctxt,
            dpiConn_create(
                ctxt.context,
                username.ptr,
                username.len,
                password.ptr,
                password.len,
                connect_string.ptr,
                connect_string.len,
                &common_params,
                &mut conn_params,
                &mut handle
            )
}

pub fn withAppContext(self: *Self, ac: [3][]const u8) void {
    self.app_context.append(ac) catch unreachable;
}

pub fn connect(self: *Self) !void {}

test "withAppContext" {
    const allocator = std.testing.allocator;
    var conn = Self{
        .username = undefined,
        .password = undefined,
        .connect_string = undefined,
        .allocator = allocator,
        .app_context = std.ArrayList([3][]const u8).init(allocator),
    };
    defer conn.app_context.deinit();

    conn.withAppContext(.{ "a", "b", "c" });
    conn.withAppContext(.{ "d", "e", "f" });

    try std.testing.expectEqual(@as(usize, 2), conn.app_context.items.len);
    try std.testing.expectEqual([3][]const u8{ "a", "b", "c" }, conn.app_context.items[0]);
    try std.testing.expectEqual([3][]const u8{ "d", "e", "f" }, conn.app_context.items[1]);
}
