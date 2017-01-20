
-- Sets a database and hostname as a meta node.
CREATE OR REPLACE FUNCTION set_meta(
    database_name NAME,
    hostname      TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    meta_row _iobeamdb_catalog.meta;
BEGIN
    SELECT *
    INTO meta_row
    FROM _iobeamdb_catalog.meta
    LIMIT 1;

    IF meta_row IS NULL THEN
        INSERT INTO _iobeamdb_catalog.meta (database_name, hostname, server_name)
        VALUES (database_name, hostname, database_name);
    ELSE
        IF meta_row.database_name <> database_name OR meta_row.hostname <> hostname THEN
            RAISE EXCEPTION 'Changing meta info is not supported'
            USING ERRCODE = 'IO101';
        END IF;
    END IF;
END
$BODY$;

-- Adds a new node to the cluster, with its database name and hostname.
CREATE OR REPLACE FUNCTION add_node(
    database_name NAME,
    hostname      TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    schema_name := format('remote_%s', database_name);
    IF database_name = current_database() THEN
        schema_name = 'public';
    END IF;
    INSERT INTO _iobeamdb_catalog.node (database_name, schema_name, server_name, hostname)
    VALUES (database_name, schema_name, database_name, hostname)
    ON CONFLICT DO NOTHING;
END
$BODY$;

-- Adds new user credentials for the cluster.
CREATE OR REPLACE FUNCTION add_cluster_user(
    username TEXT,
    password TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    INSERT INTO _iobeamdb_catalog.cluster_user (username, password)
    VALUES (username, password)
    ON CONFLICT DO NOTHING;
END
$BODY$;

CREATE OR REPLACE FUNCTION add_partition_epoch(
    hypertable_name     NAME,
    keyspace_start      SMALLINT [],
    partitioning_column NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
WITH epoch AS (
    INSERT INTO _iobeamdb_catalog.partition_epoch (hypertable_name, start_time, end_time, partitioning_func, partitioning_mod, partitioning_column)
    VALUES (hypertable_name, NULL, NULL, 'get_partition_for_key', 32768, partitioning_column)
    RETURNING id
)
INSERT INTO _iobeamdb_catalog.partition (epoch_id, keyspace_start, keyspace_end)
    SELECT
        epoch.id,
        lag(start, 1, 0)
        OVER (),
        start - 1
    FROM unnest(keyspace_start :: INT [] || (32768) :: INT) start, epoch
$BODY$;

CREATE OR REPLACE FUNCTION add_equi_partition_epoch(
    hypertable_name     NAME,
    number_partitions   SMALLINT,
    partitioning_column NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT add_partition_epoch(
    hypertable_name,
    (SELECT ARRAY(SELECT start * 32768 / (number_partitions)
                  FROM generate_series(1, number_partitions - 1) AS start) :: SMALLINT []),
    partitioning_column
)
$BODY$;
