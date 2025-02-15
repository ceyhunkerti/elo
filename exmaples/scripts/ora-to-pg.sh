# make sure export LD_LIBRARY_PATH=/path_to/instantclient_<version>:$LD_LIBRARY_PATH
./zig-out/bin/elo run \
    --sink postgres \
    --sink-username $PG_TEST_USERNAME \
    --sink-password $PG_TEST_PASSWORD \
    --sink-database $PG_TEST_DATABASE \
    --sink-host $PG_TEST_HOST \
    --sink-table ora_to_pg_01 \
    --source oracle \
    --source-username $ORACLE_TEST_USERNAME \
    --source-password $ORACLE_TEST_PASSWORD \
    --source-connection-string $ORACLE_TEST_CONNECTION_STRING \
    --source-sql "select * from ora_to_pg_01"
