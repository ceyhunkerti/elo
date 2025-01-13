const std = @import("std");
const debug = std.debug;
const Allocator = std.mem.Allocator;
const testing = std.testing;
const Context = @import("Context.zig");
const t = @import("testing/testing.zig");
const Statement = @import("Statement.zig");

const c = @import("c.zig").c;
// pub const Statement = @import("Statement.zig");

const Self = @This();

pub const ConnectionError = error{
    UnknownPrivilegeMode,

    FailedToCreateConnection,
    FailedToReleaseConnection,
    FailedToDestroyContext,
    FailedToInitializeConnCreateParams,

    FailedToCommit,
    FailedToRollback,
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

    pub fn toDpi(self: Privilege) c.dpiAuthMode {
        return switch (self) {
            .SYSDBA => c.DPI_MODE_AUTH_SYSDBA,
            .SYSOPER => c.DPI_MODE_AUTH_SYSOPER,
            .SYSASM => c.DPI_MODE_AUTH_SYSASM,
            .SYSBACKUP => c.DPI_MODE_AUTH_SYSBKP,
            .SYSDG => c.DPI_MODE_AUTH_SYSDGD,
            .SYSKM => c.DPI_MODE_AUTH_SYSKMT,
            .SYSRAC => c.DPI_MODE_AUTH_SYSRAC,
            .DEFAULT => c.DPI_MODE_AUTH_DEFAULT,
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

allocator: Allocator,
dpi_conn: ?*c.dpiConn = null,
context: Context = undefined,

username: []const u8 = "",
password: []const u8 = "",
connection_string: []const u8 = "",
privilege: Privilege = .DEFAULT,

pub fn init(
    allocator: Allocator,
    username: []const u8,
    password: []const u8,
    connection_string: []const u8,
    privilege: Privilege,
) Self {
    return Self{
        .allocator = allocator,
        .username = username,
        .password = password,
        .connection_string = connection_string,
        .privilege = privilege,
    };
}
pub fn deinit(self: *Self) !void {
    if (self.dpi_conn != null) {
        if (c.dpiConn_release(self.dpi_conn) < 0) {
            return error.FailedToReleaseConnection;
        }
    }
}

fn createContext(self: *Self) !void {
    self.context = Context{};
    try self.context.create();
}

fn dpiConnCreateParams(self: *Self) !c.dpiConnCreateParams {
    var params: c.dpiConnCreateParams = undefined;
    if (c.dpiContext_initConnCreateParams(self.context.dpi_context, &params) < 0) {
        return error.FailedToInitializeConnCreateParams;
    }
    params.authMode = self.privilege.toDpi();
    return params;
}

pub fn errorMessage(self: *Self) []const u8 {
    return self.context.errorMessage();
}

pub fn connect(self: *Self) !void {
    try self.createContext();
    var create_params = try self.dpiConnCreateParams();

    var dpi_conn: ?*c.dpiConn = null;
    if (c.dpiConn_create(
        self.context.dpi_context,
        self.username.ptr,
        @intCast(self.username.len),
        self.password.ptr,
        @intCast(self.password.len),
        self.connection_string.ptr,
        @intCast(self.connection_string.len),
        null,
        &create_params,
        &dpi_conn,
    ) < 0) {
        debug.print("Failed to create connection with error: {s}\n", .{self.errorMessage()});
        return error.FailedToCreateConnection;
    }

    if (dpi_conn == null) {
        return error.FailedToCreateConnection;
    }
    self.dpi_conn = dpi_conn;
}

pub fn createStatement(self: *Self) Statement {
    return Statement.init(self.allocator, self);
}

pub fn prepareStatement(self: *Self, sql: []const u8) !Statement {
    var stmt = self.createStatement();
    try stmt.prepare(sql);
    return stmt;
}

pub fn execute(self: *Self, sql: []const u8) !void {
    var stmt = try self.prepareStatement(sql);
    try stmt.execute();
}

pub fn commit(self: *Self) !void {
    if (c.dpiConn_commit(self.dpi_conn) < 0) {
        debug.print("Failed to commit with error: {s}\n", .{self.getErrorMessage()});
        return error.FailedToCommit;
    }
}
// pub fn rollback(self: *Self) !void {
//     if (c.dpiConn_rollback(self.dpi_conn) < 0) {
//         debug.print("Failed to rollback with error: {s}\n", .{self.getErrorMessage()});
//         return error.FailedToRollback;
//     }
// }

// pub fn getErrorMessage(self: *Self) []const u8 {
//     var err: c.dpiErrorInfo = undefined;
//     c.dpiContext_getError(self.dpi_context, &err);
//     return std.mem.span(err.message);
// }

test "connect" {
    var conn = try t.getTestConnector(testing.allocator);
    try conn.connect();
}

// test "create context" {
//     const allocator = std.testing.allocator;
//     var connection = Self.init(allocator);
//     try connection.create_context();
// }
