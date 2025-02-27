const Connection = @This();

const std = @import("std");
const testing = std.testing;
const Context = @import("Context.zig");
const Statement = @import("Statement.zig");

const log = std.log;
const t = @import("testing/testing.zig");
const c = @import("c.zig").c;

pub const Error = error{
    FailedToCreateConnection,
    UnknownConnectionMode,
    DpiBindVariableCreationError,
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
            return error.UnknownConnectionMode;
        } else {
            return Privilege.DEFAULT;
        }
    }
};

allocator: std.mem.Allocator,
dpi_conn: ?*c.dpiConn = null,
context: Context = undefined,

username: []const u8 = "",
password: []const u8 = "",
connection_string: []const u8 = "",
privilege: Privilege = .DEFAULT,

pub fn init(
    allocator: std.mem.Allocator,
    username: []const u8,
    password: []const u8,
    connection_string: []const u8,
    privilege: Privilege,
) Connection {
    return .{
        .allocator = allocator,
        .username = username,
        .password = password,
        .connection_string = connection_string,
        .privilege = privilege,
    };
}
pub fn deinit(self: *Connection) void {
    if (self.dpi_conn != null) {
        if (c.dpiConn_release(self.dpi_conn) < 0) {
            log.err("Failed to release connection with error: {s}\n", .{self.context.errorMessage()});
            unreachable;
        }
        self.dpi_conn = null;
    }
}

fn createContext(self: *Connection) !void {
    self.context = Context{};
    try self.context.create();
}

fn dpiConnCreateParams(self: *Connection) !c.dpiConnCreateParams {
    var params: c.dpiConnCreateParams = undefined;
    if (c.dpiContext_initConnCreateParams(self.context.dpi_context, &params) < 0) {
        return error.Fail;
    }
    params.authMode = self.privilege.toDpi();
    return params;
}

pub fn errorMessage(self: Connection) []const u8 {
    return self.context.errorMessage();
}

pub fn connect(self: *Connection) !void {
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
        log.err("Failed to create connection with error: {s}\n", .{self.errorMessage()});
        return error.FailedToCreateConnection;
    }

    if (dpi_conn == null) {
        return error.FailedToCreateConnection;
    }
    self.dpi_conn = dpi_conn;
}
test "connect" {
    var cp = try t.ConnectionParams.initFromEnv(std.testing.allocator);
    var conn = cp.toConnection();
    try conn.connect();
    conn.deinit();
}

pub fn isConnected(self: Connection) bool {
    return self.dpi_conn != null;
}

pub fn createStatement(self: *Connection) Statement {
    return Statement.init(self.allocator, self);
}

pub fn prepareStatement(self: *Connection, sql: []const u8) !Statement {
    var stmt = self.createStatement();
    try stmt.prepare(sql);
    return stmt;
}

pub fn execute(self: *Connection, sql: []const u8) !u32 {
    var stmt = try self.prepareStatement(sql);
    return try stmt.execute();
}

pub fn commit(self: Connection) !void {
    if (c.dpiConn_commit(self.dpi_conn) < 0) {
        return error.Fail;
    }
}
pub fn rollback(self: Connection) !void {
    if (c.dpiConn_rollback(self.dpi_conn) < 0) {
        return error.Fail;
    }
}

pub fn newDpiVariable(
    self: Connection,
    dpi_oracle_type: c.dpiOracleTypeNum,
    dpi_native_type: c.dpiNativeTypeNum,
    max_array_size: u32,
    size: u32,
    size_is_bytes: bool,
    is_array: bool,
    obj_type: ?*c.dpiObjectType,
    @"var": [*c]?*c.dpiVar,
    data: [*c][*c]c.dpiData,
) !void {
    if (c.dpiConn_newVar(
        self.dpi_conn,
        dpi_oracle_type,
        dpi_native_type,
        max_array_size,
        size,
        if (size_is_bytes) 1 else 0,
        if (is_array) 1 else 0,
        obj_type,
        @"var",
        data,
    ) < 0) {
        return error.DpiBindVariableCreationError;
    }
}

pub fn count(self: *Connection, table_name: []const u8) !f64 {
    const sql = try std.fmt.allocPrint(self.allocator, "select count(*) from {s}", .{table_name});
    defer self.allocator.free(sql);
    var stmt = try self.prepareStatement(sql);
    const record = try stmt.fetch(try stmt.execute());
    if (record) |r| {
        defer r.deinit(self.allocator);
        return r.get(0).Double.?;
    } else {
        return error.Fail;
    }
}

pub fn truncate(self: *Connection, table_name: []const u8) !void {
    const sql = try std.fmt.allocPrint(self.allocator, "truncate table {s}", .{table_name});
    defer self.allocator.free(sql);
    _ = try self.execute(sql);
}
