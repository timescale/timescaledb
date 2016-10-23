CREATE OR REPLACE FUNCTION get_partition_table_name(
    namespace_name   NAME,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('partition_%s_%s', partition_number, total_partitions)::NAME;
$BODY$;

CREATE OR REPLACE FUNCTION get_data_table_name(
    namespace_name     NAME,
    partition_number   SMALLINT,
    total_partitions   SMALLINT,
    partitioning_field NAME,
    table_start        BIGINT,
    table_end          BIGINT
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN table_start IS NOT NULL THEN
           format('data_%s_%s_%s', partition_number, total_partitions, table_start / 1e9 :: BIGINT)::NAME
       ELSE
           format('data_%s_%s_%s', partition_number, total_partitions, table_end / 1e9 :: BIGINT)::NAME
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_data_table_primary_key_index_name(
    namespace_name     NAME,
    partition_number   SMALLINT,
    total_partitions   SMALLINT,
    partitioning_field NAME,
    table_start        BIGINT,
    table_end          BIGINT
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('%s_pidx', get_data_table_name(namespace_name, partition_number, total_partitions, partitioning_field, table_start, table_end))::NAME
$BODY$;