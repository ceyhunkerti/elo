const connection = @import("connection.zig");
pub const getTestConnection = connection.getTestConnection;
pub const getTestConnectionParams = connection.getTestConnectionParams;

pub fn schema() []const u8 {
    const p = getTestConnectionParams() catch unreachable;
    return p.username;
}
