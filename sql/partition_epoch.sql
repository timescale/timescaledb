-- Adds a new partition epoch. Used when changing partitioning.
CREATE OR REPLACE FUNCTION add_partition_epoch(
    hypertable_id               INTEGER,
    keyspace_start              SMALLINT [],
    number_partitions           SMALLINT,
    partitioning_column         NAME,
    partitioning_func_schema    NAME,
    partitioning_func           NAME,
    tablespace_name             NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    WITH epoch AS (
        INSERT INTO _timescaledb_catalog.partition_epoch (hypertable_id, start_time, end_time, num_partitions,
            partitioning_func_schema, partitioning_func, partitioning_mod, partitioning_column)
        VALUES (hypertable_id, NULL, NULL, number_partitions, partitioning_func_schema,
        partitioning_func, 32768, partitioning_column)
        RETURNING id
    ), hypertable AS (
        SELECT * FROM _timescaledb_catalog.hypertable WHERE id = hypertable_id
    )
    INSERT INTO _timescaledb_catalog.partition (epoch_id, keyspace_start, keyspace_end, tablespace)
    SELECT
        epoch.id,
        lag(start, 1, 0)
        OVER (),
        start - 1,
        tablespace_name
    FROM unnest(keyspace_start :: INT [] || (32768) :: INT) start, epoch, hypertable
$BODY$;

-- Add a new partition epoch with equally sized partitions
CREATE OR REPLACE FUNCTION add_equi_partition_epoch(
    hypertable_id               INTEGER,
    number_partitions           SMALLINT,
    partitioning_column         NAME,
    partitioning_func_schema    NAME,
    partitioning_func           NAME,
    tablespace_name             NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT add_partition_epoch(
    hypertable_id,
    (SELECT ARRAY(SELECT start * 32768 / (number_partitions)
                  FROM generate_series(1, number_partitions - 1) AS start) :: SMALLINT []),
    number_partitions,
    partitioning_column,
    partitioning_func_schema,
    partitioning_func,
    tablespace_name
)
$BODY$;
