# make sure export LD_LIBRARY_PATH=/path_to/instantclient_<version>:$LD_LIBRARY_PATH
./zig-out/bin/elo run \
    --source postgres \
    --source-username $PG_TEST_USERNAME \
    --source-password $PG_TEST_PASSWORD \
    --source-database $PG_TEST_DATABASE \
    --source-host $PG_TEST_HOST \
    --source-sql "select * from ora_to_pg_01 order by a" \
    --sink oracle \
    --sink-username $ORACLE_TEST_USERNAME \
    --sink-password $ORACLE_TEST_PASSWORD \
    --sink-connection-string $ORACLE_TEST_CONNECTION_STRING \
    --sink-table pg_to_ora_02
