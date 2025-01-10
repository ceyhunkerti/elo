const std = @import("std");
const debug = std.debug;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const t = @import("testing/testing.zig");

const c = @import("c.zig").c;
pub const Statement = @import("Statement.zig");

const Self = @This();

allocator: Allocator,
conn: ?*c.dpiConn = null,
dpi_context: ?*c.dpiContext = null,

const ConnectionError = error{
    FailedToCreateConnection,
    FailedToCreateContext,
    FailedToReleaseConnection,
    FailedToDestroyContext,
    FailedToInitializeConnCreateParams,
};

const TransactionError = error{
    FailedToCommit,
    FailedToRollback,
};

pub fn init(allocator: Allocator) Self {
    return Self{
        .conn = null,
        .dpi_context = null,
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self) !void {
    if (self.conn != null) {
        if (c.dpiConn_release(self.conn) < 0) {
            debug.print("Failed to release connection: {s}\n", .{self.getErrorMessage()});
            return ConnectionError.FailedToReleaseConnection;
        }
    }
}

pub fn create_context(self: *Self) ConnectionError!void {
    if (self.dpi_context != null) {
        return;
    }
    var err: c.dpiErrorInfo = undefined;
    if (c.dpiContext_createWithParams(c.DPI_MAJOR_VERSION, c.DPI_MINOR_VERSION, null, &self.dpi_context, &err) < 0) {
        debug.print("Failed to create context with error: {s}\n", .{err.message});
        return ConnectionError.FailedToCreateContext;
    }
}

fn connCreateParams(self: *Self, auth_mode_int: u32) ConnectionError!c.dpiConnCreateParams {
    var params: c.dpiConnCreateParams = undefined;
    if (c.dpiContext_initConnCreateParams(self.dpi_context, &params) < 0) {
        debug.print("Failed to initialize connection create params\n", .{});
        return ConnectionError.FailedToInitializeConnCreateParams;
    }
    params.authMode = auth_mode_int;

    return params;
}

pub fn connect(
    self: *Self,
    username: []const u8,
    password: []const u8,
    connection_string: []const u8,
    auth_mode_int: u32,
) ConnectionError!void {
    try self.create_context();

    var params = try self.connCreateParams(auth_mode_int);
    if (c.dpiConn_create(
        self.dpi_context,
        username.ptr,
        @intCast(username.len),
        password.ptr,
        @intCast(password.len),
        connection_string.ptr,
        @intCast(connection_string.len),
        null,
        &params,
        &self.conn,
    ) < 0) {
        debug.print("Failed to create connection with error: {s}\n", .{self.getErrorMessage()});
        return ConnectionError.FailedToCreateConnection;
    }
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

pub fn commit(self: *Self) TransactionError!void {
    if (c.dpiConn_commit(self.conn) < 0) {
        debug.print("Failed to commit with error: {s}\n", .{self.getErrorMessage()});
        return TransactionError.FailedToCommit;
    }
}
pub fn rollback(self: *Self) TransactionError!void {
    if (c.dpiConn_rollback(self.conn) < 0) {
        debug.print("Failed to rollback with error: {s}\n", .{self.getErrorMessage()});
        return TransactionError.FailedToRollback;
    }
}

pub fn getErrorMessage(self: *Self) []const u8 {
    var err: c.dpiErrorInfo = undefined;
    c.dpiContext_getError(self.dpi_context, &err);
    return std.mem.span(err.message);
}

test "connect" {
    var conn = try t.getTestConnection(testing.allocator);
    try conn.deinit();
}

test "create context" {
    const allocator = std.testing.allocator;
    var connection = Self.init(allocator);
    try connection.create_context();
}
