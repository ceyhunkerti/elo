const std = @import("std");
const oracle = @import("../oracle/oracle.zig");
const postgres = @import("../postgres/postgres.zig");

test "postgres to oracle" {
    const allocator = std.testing.allocator;

    const pg_options = postgres.options.SourceOptions{
        .sql = "select * from TEST_PG_TO_ORACLE_01",
        .connection = postgres.t.connectionOptions(allocator),
    };
    const ora_options = oracle.options.SinkOptions{
        .connection = oracle.t.connectionOptions(allocator),
        .table = "TEST_PG_TO_ORACLE_01",
    };

    //     // postgres table create script
    const postgres_table_script =
        \\create table TEST_PG_TO_ORACLE_01 (
        \\  C_INT INT,
        \\  C_NUM NUMERIC,
        \\  C_VARCHAR VARCHAR(50),
        \\  C_DATE DATE,
        \\  C_BOOL BOOLEAN,
        \\  C_TIMESTAMP TIMESTAMP,
        \\  C_TIMESTAMP_TZ TIMESTAMP WITH TIME ZONE,
        \\  C_TEXT TEXT
        \\)
    ;

    const oracle_table_script =
        \\create table TEST_PG_TO_ORACLE_01 (
        \\  C_INT NUMBER,
        \\  C_NUM NUMBER,
        \\  C_VARCHAR VARCHAR2(50),
        \\  C_DATE DATE,
        \\  C_BOOL CHAR(1),
        \\  C_TIMESTAMP TIMESTAMP,
        \\  C_TIMESTAMP_TZ TIMESTAMP WITH TIME ZONE,
        \\  C_TEXT CLOB
        \\)
    ;

    var pg_reader = postgres.Reader.init(allocator, pg_options);
    defer pg_reader.deinit();
    var ora_writer = oracle.Writer.init(allocator, ora_options);
    defer ora_writer.deinit();

    try pg_reader.connect();
    const is_pg_table_exists = try postgres.t.isTableExists(allocator, &pg_reader.conn, "TEST_PG_TO_ORACLE_01");

    if (is_pg_table_exists) {
        try postgres.t.dropTable(allocator, &pg_reader.conn, "TEST_PG_TO_ORACLE_01");
    } else {
        try postgres.t.createTable(&pg_reader.conn, postgres_table_script);
    }

    var oracle_tt = oracle.t.TestTable.init(allocator, &ora_writer.conn, "TEST_PG_TO_ORACLE_01", oracle_table_script);
    defer oracle_tt.deinit();
}
