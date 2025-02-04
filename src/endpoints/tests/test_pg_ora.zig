const std = @import("std");
const w = @import("../../wire/wire.zig");
const oracle = @import("../oracle/oracle.zig");
const postgres = @import("../postgres/postgres.zig");

test "postgres to oracle" {
    const allocator = std.testing.allocator;

    const ora_options = oracle.options.SinkOptions{
        .connection = oracle.t.connectionOptions(allocator),
        .table = "TEST_PG_TO_ORACLE_01",
    };

    const oracle_table_script =
        \\create table TEST_PG_TO_ORACLE_01 (
        \\  C_INT NUMBER,
        \\  C_NUM NUMBER,
        \\  C_VARCHAR VARCHAR2(50),
        \\  C_DATE DATE,
        \\  C_BOOL CHAR(1),
        \\  C_TIMESTAMP TIMESTAMP,
        \\  C_TIMESTAMP_TZ TIMESTAMP WITH TIME ZONE,
        \\  C_TEXT VARCHAR2(4000)
        \\)
    ;

    var ora_writer = oracle.Writer.init(allocator, ora_options);
    defer ora_writer.deinit();

    try ora_writer.connect();
    var oracle_tt = oracle.t.TestTable.init(allocator, &ora_writer.conn, "TEST_PG_TO_ORACLE_01", oracle_table_script);
    defer oracle_tt.deinit();

    try oracle_tt.createIfNotExists();
    // defer oracle_tt.dropIfExists() catch unreachable;

    var pg_select_sql = std.ArrayList(u8).init(allocator);
    defer pg_select_sql.deinit();
    for (0..1_000) |i| {
        try pg_select_sql.writer().print(
            \\SELECT 1::int as C_int
            \\, {d}::numeric as C_num
            \\, 'test'::varchar as C_varchar
            \\, '2024-01-01'::date as C_date
            \\, true::boolean as C_bool
            \\, '2024-01-01 12:00:00'::timestamp as C_timestamp
            \\, '2024-01-01 12:00:00+03'::timestamp with time zone as C_timestamp_tz
            \\, 'test'::text as C_text
        , .{i});
        if (i < 999) {
            try pg_select_sql.appendSlice(" \nunion all\n");
        }
    }
    try pg_select_sql.appendSlice("\x00");

    const pg_options = postgres.options.SourceOptions{
        .sql = pg_select_sql.items.ptr[0 .. pg_select_sql.items.len - 1 :0],
        .connection = postgres.t.connectionOptions(allocator),
    };
    std.debug.print("{s}\n", .{pg_options.sql});

    var pg_reader = postgres.Reader.init(allocator, pg_options);
    defer pg_reader.deinit();
    try pg_reader.connect();

    var wire = w.Wire.init();

    try pg_reader.run(&wire);
    wire.put(w.Term(allocator));
    try ora_writer.run(&wire);
}
