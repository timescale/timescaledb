CREATE OR REPLACE FUNCTION _timescaledb_internal.create_user_mapping(
    cluster_user_row _timescaledb_catalog.cluster_user,
    server_name      NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE USER MAPPING FOR %1$I SERVER %2$I OPTIONS (user '%1$s', password '%3$s')
        $$, cluster_user_row.username, server_name, cluster_user_row.password);
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_server(
    server_name   NAME,
    hostname      NAME,
    port          INT,
    database_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host '%s', dbname '%s', port '%s');
        $$, server_name, hostname, database_name, port);
END
$BODY$;
