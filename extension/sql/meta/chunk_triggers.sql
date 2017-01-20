CREATE OR REPLACE FUNCTION _meta.place_chunks(
    chunk_row _iobeamdb_catalog.chunk,
    placement chunk_placement_type,
    replication_factor SMALLINT
)
    RETURNS TABLE(replica_id SMALLINT, database_name NAME) LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
  PERFORM setseed(chunk_row.id::double precision/2147483647::double precision);
  IF placement = 'RANDOM' THEN
      RETURN QUERY
        SELECT pr.replica_id, dn.database_name
        FROM _iobeamdb_catalog.partition_replica pr
        INNER JOIN (
          SELECT *
          FROM
          (
            SELECT DISTINCT n.database_name
            FROM _iobeamdb_catalog.node n
            LIMIT replication_factor
          ) AS d
          ORDER BY random()
        ) AS dn ON TRUE
        WHERE pr.partition_id = chunk_row.partition_id;
  ELSIF placement = 'STICKY' THEN
      RETURN QUERY
          SELECT pr.replica_id, dn.database_name
          FROM _iobeamdb_catalog.partition_replica pr
          INNER JOIN LATERAL (
              SELECT crn.database_name
              FROM _iobeamdb_catalog.chunk_replica_node crn
              INNER JOIN _iobeamdb_catalog.chunk c ON (c.id = crn.chunk_id)
              WHERE crn.partition_replica_id = pr.id
              ORDER BY GREATEST(chunk_row.start_time, chunk_row.end_time) - GREATEST(c.start_time, c.end_time) ASC NULLS LAST
              LIMIT 1
          ) AS dn ON true
          WHERE pr.partition_id = chunk_row.partition_id;
      IF NOT FOUND THEN
        RETURN query SELECT *
        FROM _meta.place_chunks(chunk_row, 'RANDOM', replication_factor);
      END IF;
  END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _meta.on_create_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    column_row  _iobeamdb_catalog.hypertable_column;
    schema_name NAME;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF (
               (OLD.start_time IS NULL AND new.start_time IS NOT NULL)
               OR
               (OLD.end_time IS NULL AND new.end_time IS NOT NULL)
           )
           AND (
               OLD.id = NEW.id AND
               OLD.partition_id = NEW.partition_id
           ) THEN
            NULL;
        ELSE
            RAISE EXCEPTION 'This type of update not allowed on % table', TG_TABLE_NAME
            USING ERRCODE = 'IO101';
        END IF;
    ELSIF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts and updates supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    --sync data on insert
    IF TG_OP = 'INSERT' THEN
        FOR schema_name IN
        SELECT n.schema_name
        FROM _iobeamdb_catalog.node AS n
        WHERE n.database_name <> current_database()
        LOOP
            EXECUTE format(
              $$
                  INSERT INTO %I.%I SELECT $1.*
              $$,
                  schema_name,
                  TG_TABLE_NAME
            )
            USING NEW;
        END LOOP;

        --do not sync data on update. synced by close_chunk logic.

        INSERT INTO _iobeamdb_catalog.chunk_replica_node (chunk_id, partition_replica_id, database_name, schema_name, table_name)
            SELECT
                NEW.id,
                pr.id,
                p.database_name,
                pr.schema_name,
                format('%s_%s_%s_%s_data', h.associated_table_prefix, pr.id, pr.replica_id, NEW.id)
            FROM _iobeamdb_catalog.partition_replica pr
            INNER JOIN _iobeamdb_catalog.hypertable h ON (h.name = pr.hypertable_name)
            INNER JOIN _meta.place_chunks(new, h.placement, h.replication_factor) p ON (p.replica_id = pr.replica_id)
            WHERE pr.partition_id = NEW.partition_id;
    END IF;

    RETURN NEW;
END
$BODY$;
