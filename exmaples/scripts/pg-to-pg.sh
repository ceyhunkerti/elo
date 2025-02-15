# make sure export LD_LIBRARY_PATH=/path_to/instantclient_<version>:$LD_LIBRARY_PATH
./zig-out/bin/elo run \
    --source postgres \
    --source-username $PG_TEST_USERNAME \
    --source-password $PG_TEST_PASSWORD \
    --source-database $PG_TEST_DATABASE \
    --source-host $PG_TEST_HOST \
    --source-sql "select * from ora_to_pg_01 order by a" \
    --sink postgres \
    --sink-username $PG_TEST_USERNAME \
    --sink-password $PG_TEST_PASSWORD \
    --sink-database $PG_TEST_DATABASE \
    --sink-host $PG_TEST_HOST \
    --sink-table ora_to_pg_02 \
    --sink-batch-size 5
