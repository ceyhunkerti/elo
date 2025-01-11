const std = @import("std");
const debug = std.debug;
const Allocator = std.mem.Allocator;
const commons = @import("../../commons.zig");

const Field = commons.Field;
const Metadata = commons.Metadata;
const Record = commons.Record;
pub const Connection = @import("Connection.zig");
pub const Statement = @import("Statement.zig");
const Parameter = @import("../parameters.zig").Parameter;
const EndpointType = commons.EndpointType;
const Options = @import("./options.zig").Options;
const MessageQueue = @import("../../queue.zig").MessageQueue;
const t = @import("testing/testing.zig");
const testing = std.testing;
pub const metadata = @import("./metadata.zig");

const Error = error{
    CantCallReadOnSink,
    CantCallWriteOnSource,
    MetadataNotReceived,
    NameAlreadyInUse,
    TableNotFound,
};

const params = struct {
    connection_string: Parameter = .{
        .name = "connection-string",
        .typestr = "string",
        .required = true,
        .description = "Connection string to oracle database. (example: localhost:1521/ORCLCDB)",
    },
    username: Parameter = .{
        .name = "username",
        .typestr = "string",
        .required = true,
        .description = "Username to connect to oracle database",
    },
    password: Parameter = .{
        .name = "password",
        .typestr = "string",
        .required = true,
        .description = "Password to connect to oracle database",
    },
    role: Parameter = .{
        .name = "role",
        .typestr = "string",
        .required = false,
        .description = "Role to connect to oracle database",
        .default_value = .Nil,
    },
    sql: Parameter = .{
        .name = "sql",
        .typestr = "string",
        .required = false,
        .description = "Query to execute",
        .default_value = .Nil,
    },
    sink_mode: Parameter = .{
        .name = "mode",
        .typestr = "string",
        .required = false,
        .description = "Mode to use when writing to sink",
        .default_value = .Append,
    },

    pub fn table(description: ?[]const u8) Parameter {
        return .{
            .name = "table",
            .typestr = "string",
            .required = false,
            .description = if (description) |d| d else "Table to use",
            .default_value = .Nil,
        };
    }
}{};

pub const Oracle = struct {
    pub const NAME = "oracle";
    pub const DISPLAY_NAME = "Oracle Database";
    pub const DESCRIPTION = null;
    pub const SOURCE = .{
        .enabled = true,
        .max_parallel = 1,
        .parameters = &[_]Parameter{
            params.connection_string,
            params.username,
            params.password,
            params.role,
            params.sql,
        },
    };
    pub const SINK = .{
        .enabled = true,
        .max_parallel = 1,
        .parameters = &[_]Parameter{
            params.connection_string,
            params.username,
            params.password,
            params.role,
            params.table("Table to load"),
            params.sink_mode,
        },
    };

    allocator: Allocator,
    endpoint_type: EndpointType,
    options: Options,
    conn: *Connection = undefined,

    pub fn init(allocator: Allocator, options: Options) Oracle {
        const connection = allocator.create(Connection) catch unreachable;
        connection.* = .{ .allocator = allocator };

        return .{
            .allocator = allocator,
            .endpoint_type = switch (options) {
                .Source => .Source,
                .Sink => .Sink,
            },
            .options = options,
            .conn = connection,
        };
    }

    pub fn deinit(self: Oracle) !void {
        try self.conn.deinit();
    }

    pub fn read(self: Oracle, q: *MessageQueue) !void {
        if (self.endpoint_type == .Sink) {
            debug.print("Can't call read method on sink", .{});
            return Error.CantCallReadOnSink;
        }
        const sql = self.options.Source.sql;
        var stmt = try self.conn.prepareStatement(sql);
        try stmt.execute();
        while (true) {
            const record = try stmt.fetch() orelse break;
            const node = self.allocator.create(MessageQueue.Node) catch unreachable;
            node.* = .{ .data = .{ .Record = record } };
            q.put(node);
        }

        const term = self.allocator.create(MessageQueue.Node) catch unreachable;
        term.* = .{ .data = .Nil };
        q.put(term);
    }

    pub fn connect(self: Oracle) !void {
        const opts = switch (self.endpoint_type) {
            .Source => self.options.Source.connection,
            .Sink => self.options.Sink.connection,
        };
        try self.conn.connect(
            opts.username,
            opts.password,
            opts.connection_string,
            @intCast(opts.authMode()),
        );
    }

    pub fn isTableExist(self: Oracle, table: []const u8) !bool {
        const sql = try std.fmt.allocPrint(self.allocator, "select 1 from {s} where rownum = 1", .{table});
        self.conn.execute(sql) catch |err| {
            if (std.mem.indexOf(u8, self.conn.getErrorMessage(), "ORA-00942")) |_| {
                return false;
            }
            return err;
        };
        return true;
    }

    pub fn truncateTable(self: Oracle, table: []const u8) !void {
        const sql = try std.fmt.allocPrint(self.allocator, "truncate table {s}", .{table});
        try self.conn.execute(sql);
    }

    pub fn dropTable(self: Oracle, table: []const u8) !void {
        const sql = try std.fmt.allocPrint(self.allocator, "drop table {s}", .{table});
        self.conn.execute(sql) catch |err| {
            if (std.mem.indexOf(u8, self.conn.getErrorMessage(), "ORA-00942")) |_| {
                return Error.TableNotFound;
            }
            return err;
        };
    }

    pub fn createTable(self: Oracle, sql: []const u8) !void {
        self.conn.execute(sql) catch |err| {
            if (std.mem.indexOf(u8, self.conn.getErrorMessage(), "ORA-00955")) |_| {
                return Error.NameAlreadyInUse;
            }
            return err;
        };
    }

    pub fn expectMetadata(q: *MessageQueue) !Metadata {
        const message = q.get();
        switch (message.data) {
            .Metadata => |m| return m,
            else => {
                q.put(message);
                return Error.MetadataNotReceived;
            },
        }
    }

    pub fn prepareSink(self: Oracle, q: *MessageQueue) !void {
        switch (self.options.Sink.mode) {
            .Append => return,
            .Truncate => try self.truncateTable(self.options.Sink.table),
            .Create => {
                if (self.options.Sink.create_sql) |create_sql| {
                    try self.createTable(create_sql);
                    return;
                }
                const md = try self.expectMetadata(q);
                try self.dropTable(self.options.Sink.table);
                try self.createTable(metadata.Expr.init(self.allocator, md).toString());
            },
        }
    }

    pub fn buildInsertQuery(self: Oracle) ![]const u8 {
        if (self.options.Sink.sql) |sql| {
            return sql;
        }
        const table_name = self.options.Sink.table;
        const sql = try std.fmt.allocPrint(self.allocator, "select * from {s} where 1=0", .{table_name});
        var stmt = try self.conn.prepareStatement(sql);
        try stmt.execute();
        const qmd = try metadata.Query.init(self.allocator, &stmt);

        const bindings = self.allocator.alloc(u8, qmd.columnCount() * 2 - 1) catch unreachable;
        for (bindings, 0..) |*b, i| {
            b.* = if (i % 2 == 0) '?' else ',';
        }
        return try std.fmt.allocPrint(self.allocator, "insert into {s} ({s}) values ({s})", .{
            table_name,
            try std.mem.join(self.allocator, ",", try qmd.columnNames()),
            bindings,
        });
    }

    pub fn buildInsertStatement(self: Oracle) !Statement {
        const sql = self.buildInsertQuery();
        return try self.conn.prepareStatement(sql);
    }

    pub fn write(self: Oracle, q: *MessageQueue) !void {
        if (self.endpoint_type == .Source) {
            debug.print("Can't call write method on source", .{});
            return Error.CantCallWriteOnSource;
        }
        break_while: while (true) {
            const node = q.get() orelse break;
            switch (node.data) {
                // we are not interested in metadata here
                .Metadata => {},
                .Record => |record| {
                    _ = record;
                },
                .Nil => break :break_while,
            }
        }
    }
};

test "Oracle.[createTable, dropTable]" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const tp = try t.getTestConnectionParams();
    const options = Options{
        .Sink = .{
            .connection = .{
                .connection_string = tp.connection_string,
                .username = tp.username,
                .password = tp.password,
                .role = "SYSDBA",
            },
            .table = "test_table",
            .mode = .Create,
        },
    };
    var oracle = Oracle.init(allocator, options);
    try oracle.connect();
    oracle.dropTable("test_table") catch |err| {
        switch (err) {
            Error.TableNotFound => {},
            else => return err,
        }
    };
    const sql = "create table test_table (id number)";
    try oracle.createTable(sql);
    try testing.expect(try oracle.isTableExist("test_table"));
    try oracle.dropTable("test_table");
}

test "Oracle.buildInsertQuery" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const tp = try t.getTestConnectionParams();
    const options = Options{
        .Sink = .{
            .connection = .{
                .connection_string = tp.connection_string,
                .username = tp.username,
                .password = tp.password,
                .role = "SYSDBA",
            },
            .table = "test_table",
            .mode = .Create,
        },
    };
    var oracle = Oracle.init(allocator, options);
    try oracle.connect();
    oracle.dropTable("test_table") catch |err| {
        switch (err) {
            Error.TableNotFound => {},
            else => return err,
        }
    };
    const sql = "create table test_table (id number, name varchar2(100))";
    try oracle.createTable(sql);

    const iq = try oracle.buildInsertQuery();
    try testing.expectEqualStrings("insert into test_table (ID,NAME) values (?,?)", iq);
}

test "Oracle.isTableExist" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const tp = try t.getTestConnectionParams();
    const options = Options{ .Source = .{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .role = "SYSDBA",
        },
        .fetch_size = 1,
        .sql = "",
    } };
    var oracle = Oracle.init(allocator, options);
    try oracle.connect();
    try testing.expect(try oracle.isTableExist("dual"));
    try testing.expect(!try oracle.isTableExist("dual_not_exist"));
}

test "Oracle.read" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const tp = try t.getTestConnectionParams();
    const options = Options{ .Source = .{
        .connection = .{
            .connection_string = tp.connection_string,
            .username = tp.username,
            .password = tp.password,
            .role = "SYSDBA",
        },
        .fetch_size = 1,
        .sql = "select 1 as A, 2 as B from dual",
    } };

    var oracle = Oracle.init(allocator, options);
    try oracle.connect();
    var queue = MessageQueue.init();
    try oracle.read(&queue);

    var message_count: usize = 0;
    var term_received: bool = false;
    while (true) {
        switch (queue.get().data) {
            .Metadata => {},
            .Record => |record| {
                message_count += 1;
                try testing.expectEqual(record.len, 2);
                try testing.expectEqual(record[0].Double, 1);
                try testing.expectEqual(record[1].Double, 2);
            },
            .Nil => {
                term_received = true;
                break;
            },
        }
    }

    try testing.expectEqual(message_count, 1);
    try testing.expect(term_received);
    try oracle.deinit();
}

test {
    std.testing.refAllDecls(@This());
}
