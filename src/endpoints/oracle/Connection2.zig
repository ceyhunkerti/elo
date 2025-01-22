const std = @import("std");
const testing = std.testing;
const Statement = @import("Statement2.zig");

const t = @import("testing/testing.zig");
const oci = @import("c.zig").oci;

const Self = @This();

pub const ConnectionError = error{
    UnknownPrivilegeMode,
    FailedToCreateConnection,
    FailedToReleaseConnection,
    FailedToDestroyContext,
    FailedToInitializeConnCreateParams,
    FailedToCommit,
    FailedToRollback,
    FailedToCreateVariable,
};

pub const Privilege = enum {
    SYSDBA,
    SYSOPER,
    SYSASM,
    SYSBACKUP,
    SYSDG,
    SYSKM,
    SYSRAC,
    DEFAULT,

    pub fn toOci(self: Privilege) c_uint {
        return switch (self) {
            .SYSDBA => oci.OCI_SESSION_SYSDBA,
            .SYSOPER => oci.OCI_SESSION_SYSOPER,
            .SYSASM => oci.OCI_SESSION_SYSASM,
            .SYSBACKUP => oci.OCI_SESSION_SYSBKP,
            .SYSDG => oci.OCI_SESSION_SYSDGD,
            .SYSKM => oci.OCI_SESSION_SYSKMT,
            .SYSRAC => oci.OCI_SESSION_SYSRAC,
            .DEFAULT => oci.OCI_SESSION_DEFAULT,
        };
    }

    pub fn fromString(s: ?[]const u8) !Privilege {
        if (s) |str| {
            inline for (@typeInfo(Privilege).Enum.fields) |field| {
                if (std.mem.eql(u8, field.name, str)) {
                    return @field(Privilege, field.name);
                }
            }
            return error.UnknownPrivilegeMode;
        } else {
            return Privilege.DEFAULT;
        }
    }
};

allocator: std.mem.Allocator,
oci_conn: ?*oci.OCI_Connection = null,

connection_string: []const u8 = "",
username: []const u8 = "",
password: []const u8 = "",
privilege: Privilege = .DEFAULT,

pub fn init(
    allocator: std.mem.Allocator,
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
    privilege: Privilege,
) !Self {
    if (oci.OCI_Initialize(null, null, oci.OCI_ENV_DEFAULT | oci.OCI_ENV_CONTEXT) != oci.TRUE) {
        return error.Fail;
    }

    return Self{
        .allocator = allocator,
        .connection_string = connection_string,
        .username = username,
        .password = password,
        .privilege = privilege,
    };
}
pub fn deinit(self: *Self) !void {
    if (self.oci_conn) |conn| {
        _ = oci.OCI_ConnectionFree(conn);
    }
    _ = oci.OCI_Cleanup();
}

pub fn getLastError(self: Self) ![]const u8 {
    const err: ?*oci.OCI_Error = oci.OCI_GetLastError();
    if (err) |_| {
        return try std.fmt.allocPrint(self.allocator, "OCI({d}): {s}", .{
            oci.OCI_ErrorGetOCICode(err),
            oci.OCI_ErrorGetString(err),
        });
    }
    return error.Fail;
}
pub fn printLastError(self: Self) !void {
    const error_message = try self.getLastError();
    defer self.allocator.free(error_message);
    std.debug.print("\nLast Error: {s}\n", .{error_message});
}

pub fn connect(self: *Self) !void {
    if (self.oci_conn) |conn| {
        _ = oci.OCI_ConnectionFree(conn);
    }
    self.oci_conn = oci.OCI_ConnectionCreate(
        self.connection_string.ptr,
        self.username.ptr,
        self.password.ptr,
        self.privilege.toOci(),
    );

    if (self.oci_conn == null) {
        try self.printLastError();
        return error.FailedToConnect;
    }
}
test "Connection.connect" {
    const p = try t.ConnectionParams.init();

    var conn = try Self.init(testing.allocator, p.connection_string, p.username, p.password, p.privilege);
    defer conn.deinit() catch unreachable;
    try conn.connect();
}

pub fn createStatement(self: *Self) !Statement {
    var stmt = Statement.init(self.allocator, self);
    try stmt.create();
    return stmt;
}
test "Connection.createStatement" {
    const p = try t.ConnectionParams.init();
    var conn = try Self.init(testing.allocator, p.connection_string, p.username, p.password, p.privilege);
    defer conn.deinit() catch unreachable;
    try conn.connect();
    _ = try conn.createStatement();
}

pub fn prepareStatement(self: *Self, sql: []const u8) !Statement {
    var stmt = try self.createStatement();
    try stmt.prepare(sql);
    return stmt;
}
test "Connection.prepareStatement" {
    const p = try t.ConnectionParams.init();
    var conn = try Self.init(testing.allocator, p.connection_string, p.username, p.password, p.privilege);
    defer conn.deinit() catch unreachable;
    try conn.connect();
    _ = try conn.prepareStatement("select 1 from dual");
}

pub fn commit(self: Self) !void {
    if (self.oci_conn) |conn| {
        if (oci.OCI_Commit(conn) != oci.TRUE) {
            return error.Fail;
        }
    } else {
        std.debug.print("Connection is not initialized\n", .{});
        return error.Fail;
    }
}
