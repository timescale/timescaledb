CREATE OR REPLACE FUNCTION _timescaledb_meta.place_chunks(
    chunk_row          _timescaledb_catalog.chunk,
    placement          _timescaledb_catalog.chunk_placement_type,
    replication_factor SMALLINT
)
    RETURNS TABLE(replica_id SMALLINT, database_name NAME) LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
  PERFORM setseed(chunk_row.id::double precision/2147483647::double precision);
  IF placement = 'RANDOM' THEN
      --place randomly on nodes with the same number of crns (in the same epoch) already on that node
      --but prefer nodes with less crns on them.
      RETURN QUERY
        SELECT pr.replica_id, dn.database_name
        FROM _timescaledb_catalog.partition_replica pr
        INNER JOIN (
          SELECT d.database_name
          FROM
          (
            SELECT DISTINCT ON (n.database_name) n.database_name, crns_already_on_node.crn_cnt AS current_crn_count
            FROM _timescaledb_catalog.node n
            , LATERAL (
                SELECT count(*) AS crn_cnt
                FROM _timescaledb_catalog.chunk_replica_node crn
                INNER JOIN _timescaledb_catalog.chunk c ON (c.id = crn.chunk_id)
                INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
                INNER JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id=p.epoch_id)
                WHERE crn.database_name = n.database_name
                      AND pe.id = (
                        SELECT my_pe.id
                        FROM _timescaledb_catalog.chunk my_c
                        INNER JOIN _timescaledb_catalog.partition my_p ON (my_p.id = my_c.partition_id)
                        INNER JOIN _timescaledb_catalog.partition_epoch my_pe ON (my_pe.id=my_p.epoch_id)
                        WHERE my_c.id = chunk_row.id
                    )
            ) AS crns_already_on_node
          ) AS d
          ORDER BY (current_crn_count + random()) ASC
          LIMIT replication_factor
        ) AS dn ON TRUE
        WHERE pr.partition_id = chunk_row.partition_id;
  ELSIF placement = 'STICKY' THEN
      RETURN QUERY
          SELECT pr.replica_id, dn.database_name
          FROM _timescaledb_catalog.partition_replica pr
          INNER JOIN LATERAL (
              SELECT crn.database_name
              FROM _timescaledb_catalog.chunk_replica_node crn
              INNER JOIN _timescaledb_catalog.chunk c ON (c.id = crn.chunk_id)
              WHERE crn.partition_replica_id = pr.id
              ORDER BY GREATEST(chunk_row.start_time, chunk_row.end_time) - GREATEST(c.start_time, c.end_time) ASC NULLS LAST
              LIMIT 1
          ) AS dn ON true
          WHERE pr.partition_id = chunk_row.partition_id;
      IF NOT FOUND THEN
        RETURN query SELECT *
        FROM _timescaledb_meta.place_chunks(chunk_row, 'RANDOM', replication_factor);
      END IF;
  END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_meta.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    IF TG_OP = 'DELETE' THEN
        FOR schema_name IN
        SELECT n.schema_name
        FROM _timescaledb_catalog.node AS n
        LOOP
            EXECUTE format(
                $$
                DELETE FROM %I.%I WHERE id = %L
                $$,
                schema_name,
                TG_TABLE_NAME,
                OLD.id
            );
        END LOOP;
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

        IF current_setting('timescaledb_internal.originating_node') = 'on' THEN
            INSERT INTO _timescaledb_catalog.chunk_replica_node (chunk_id, partition_replica_id, database_name, schema_name, table_name)
                SELECT
                    NEW.id,
                    pr.id,
                    p.database_name,
                    pr.schema_name,
                    format('%s_%s_%s_%s_data', h.associated_table_prefix, pr.id, pr.replica_id, NEW.id)
                FROM _timescaledb_catalog.partition_replica pr
                INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = pr.hypertable_id)
                INNER JOIN _timescaledb_meta.place_chunks(new, h.placement, h.replication_factor) p ON (p.replica_id = pr.replica_id)
                WHERE pr.partition_id = NEW.partition_id;
        END IF;
    END IF;

    RETURN NEW;
END
$BODY$;
