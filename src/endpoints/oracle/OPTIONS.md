connection:
    connection_string: []const u8,
    username: []const u8,
    password: []const u8,
    privilege: Privilege = .DEFAULT,


source:


target:
    connection: Connection
    table_name: ?[]const u8
    columns: ?[]const []const u8
    mode: (append, truncate)
    sql: ?[]const u8
    batch_size: u32 = 10_000
