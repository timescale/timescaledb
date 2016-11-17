CREATE OR REPLACE FUNCTION set_meta(
    database_name NAME,
    hostname      TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
  meta_row meta;
BEGIN
    SELECT *
    INTO meta_row
    FROM meta
    LIMIT 1;

    IF meta_row IS NULL THEN 
      INSERT INTO meta (database_name, hostname, server_name)
      VALUES (database_name, hostname, database_name);
    ELSE
      IF meta_row.database_name <> database_name OR meta_row.hostname <> hostname THEN
        RAISE EXCEPTION 'Changing meta info is not supported' USING ERRCODE = 'IO101';
      END IF;
    END IF;
END
$BODY$;

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
    INSERT INTO node (database_name, schema_name, server_name, hostname)
    VALUES (database_name, schema_name, database_name, hostname)
    ON CONFLICT DO NOTHING;
END
$BODY$;

CREATE OR REPLACE FUNCTION add_cluster_user(
    username TEXT,
    password TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    INSERT INTO cluster_user (username, password)
    VALUES (username, password)
    ON CONFLICT DO NOTHING;
END
$BODY$;


CREATE SEQUENCE IF NOT EXISTS default_hypertable_seq;

CREATE OR REPLACE FUNCTION add_hypertable(
    hypertable_name  NAME,
    partitioning_field NAME,
    main_schema_name NAME = 'public',
    associated_schema_name NAME = NULL,
    associated_table_prefix NAME = NULL,
    number_partitions SMALLINT = NULL,
    replication_factor SMALLINT = 1
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
  id INTEGER;
BEGIN
  
  id :=  nextval('default_hypertable_seq');

  IF associated_schema_name IS NULL THEN
     associated_schema_name = format('_sys_%s_%s', id, hypertable_name);
  END IF;

  IF  associated_table_prefix IS NULL THEN
     associated_table_prefix = format('_hyper_%s', id);
  END IF;

  IF number_partitions IS NULL THEN
    SELECT COUNT(*) INTO number_partitions
    FROM node;
  END IF;

  INSERT INTO hypertable (
    name, 
    main_schema_name, main_table_name, 
    associated_schema_name, associated_table_prefix, 
    root_schema_name, root_table_name,
    distinct_schema_name, distinct_table_name,
    replication_factor)
  VALUES (
    hypertable_name, 
    main_schema_name, hypertable_name,
    associated_schema_name, associated_table_prefix, 
    associated_schema_name, format('%s_root',associated_table_prefix),
    associated_schema_name, format('%s_distinct',associated_table_prefix),
    replication_factor)
  ON CONFLICT DO NOTHING;

  IF number_partitions != 0 THEN
    PERFORM add_equi_partition_epoch(hypertable_name, number_partitions, partitioning_field);
  END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION add_partition_epoch(
    hypertable_name  NAME,
    keyspace_start SMALLINT[],
    partitioning_field NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
WITH epoch AS (
  INSERT INTO partition_epoch (hypertable_name, start_time, end_time, partitioning_func, partitioning_mod, partitioning_field)
  VALUES (hypertable_name, NULL, NULL,  'get_partition_for_key', 32768, partitioning_field) RETURNING id
)
INSERT INTO partition(epoch_id, keyspace_start, keyspace_end)
  SELECT epoch.id, lag(start,1,0) OVER (), start-1
  FROM unnest(keyspace_start::int[] || (32768)::INT) start, epoch
$BODY$;

CREATE OR REPLACE FUNCTION add_equi_partition_epoch(
hypertable_name     NAME,
number_partitions   SMALLINT,
partitioning_field  NAME
)
RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT add_partition_epoch(
  hypertable_name,
  (SELECT ARRAY(SELECT start * 32768/(number_partitions) from generate_series(1, number_partitions-1) as start)::SMALLINT[]),
  partitioning_field
)
$BODY$;

CREATE OR REPLACE FUNCTION add_field(
    hypertable_name  NAME,
    field_name      NAME,
    data_type       REGTYPE,
    is_partitioning BOOLEAN,
    is_distinct     BOOLEAN,
    idx_types       field_index_type []
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO field (hypertable_name, name, data_type, is_partitioning, is_distinct, index_types)
VALUES (hypertable_name, field_name, data_type, is_partitioning, is_distinct, idx_types)
ON CONFLICT DO NOTHING;
$BODY$;

