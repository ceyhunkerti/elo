const Connection = @import("../Connection.zig");
const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log;
const c = @import("../c.zig").c;
const e = @import("../error.zig");
const base = @import("base");
const pgtype = @import("pgtype.zig");

pub fn findQueryMetadata(allocator: Allocator, conn: *Connection, query: [:0]const u8) !base.QueryMetadata(pgtype.PostgresType) {
    const res = c.PQexec(conn.pg_conn, query);
    defer c.PQclear(res);

    if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
        log.err("Error executing SQL: {s}\n", .{e.resultError(res)});
        return error.Fail;
    }

    const column_count: u32 = @intCast(c.PQnfields(res));
    const qmd = try base.QueryMetadata(pgtype.PostgresType).init(allocator, column_count, query);
    for (qmd.columns, 0..) |*column, i| {
        const type_oid = c.PQftype(res, @intCast(i));
        const column_name = std.mem.span(c.PQfname(res, @intCast(i)));

        column.* = .{
            .allocator = allocator,
            .index = i,
            .name = try allocator.dupe(u8, column_name),
            .type_info = .{
                .ext = pgtype.PostgresType.fromOid(type_oid) orelse {
                    log.err("Error: could not find type for OID {d}\n", .{type_oid});
                    return error.Fail;
                },
            },
        };
    }

    qmd.count = @intCast(c.PQntuples(res));

    return qmd;
}
