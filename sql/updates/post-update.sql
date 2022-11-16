-- needed post 1.7.0 to fixup continuous aggregates created in 1.7.0 ---
DO $$
DECLARE
 vname regclass;
 materialized_only bool;
 ts_version TEXT;
BEGIN
    SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';
    IF ts_version >= '2.7.0' THEN
            CREATE PROCEDURE _timescaledb_internal.post_update_cagg_try_repair(
                cagg_view REGCLASS
            ) AS '@MODULE_PATHNAME@', 'ts_cagg_try_repair' LANGUAGE C;
    END IF;
    FOR vname, materialized_only IN select format('%I.%I', cagg.user_view_schema, cagg.user_view_name)::regclass, cagg.materialized_only from _timescaledb_catalog.continuous_agg cagg
    LOOP
        -- the cast from oid to text returns
        -- quote_qualified_identifier (see regclassout).
        --
        -- We use the if statement to handle pre-2.0 as well as
        -- post-2.0.  This could be turned into a procedure if we want
        -- to have something more generic, but right now it is just
        -- this case.
        IF ts_version < '2.0.0' THEN
            EXECUTE format('ALTER VIEW %s SET (timescaledb.materialized_only=%L) ', vname::text, materialized_only);
        ELSIF ts_version < '2.7.0' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW %s SET (timescaledb.materialized_only=%L) ', vname::text, materialized_only);
        ELSE
            SET log_error_verbosity TO VERBOSE;
            CALL _timescaledb_internal.post_update_cagg_try_repair(vname);
        END IF;
    END LOOP;
    IF ts_version >= '2.7.0' THEN
            DROP PROCEDURE IF EXISTS _timescaledb_internal.post_update_cagg_try_repair;
    END IF;
    EXCEPTION WHEN OTHERS THEN RAISE;
END
$$;

-- can only be dropped after views have been rebuilt
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_watermark(oid);

-- For objects that are newly created, we need to set the initprivs to
-- the initprivs for some table that was created in the installation
-- of the TimescaleDB extension and not as part of any update.
--
-- We chose the "chunk" catalog table for this since that is created
-- in the first version of TimescaleDB and should have the correct
-- initprivs, but we could use any other table that existed in the
-- first installation.
INSERT INTO _timescaledb_internal.saved_privs
     SELECT nspname, relname, relacl,
       (SELECT tmpini FROM _timescaledb_internal.saved_privs
        WHERE tmpnsp = '_timescaledb_catalog' AND tmpname = 'chunk')
       FROM pg_class JOIN pg_namespace ns ON ns.oid = relnamespace
         LEFT JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname
      WHERE relkind IN ('r', 'v') AND nspname IN ('_timescaledb_catalog', '_timescaledb_config')
        OR nspname = '_timescaledb_internal'
        AND relname IN ('hypertable_chunk_local_size', 'compressed_chunk_stats',
                        'bgw_job_stat', 'bgw_policy_chunk_stats')
ON CONFLICT DO NOTHING;

-- The above is good enough for tables and views. However sequences need to
-- use the "chunk_id_seq" catalog sequence as a template
INSERT INTO _timescaledb_internal.saved_privs
     SELECT nspname, relname, relacl,
        (SELECT tmpini FROM _timescaledb_internal.saved_privs
	     WHERE tmpnsp = '_timescaledb_catalog' AND tmpname = 'chunk_id_seq')
        FROM pg_class JOIN pg_namespace ns ON ns.oid = relnamespace
		    LEFT JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname
      WHERE relkind IN ('S') AND nspname IN ('_timescaledb_catalog', '_timescaledb_config')
        OR nspname = '_timescaledb_internal'
        AND relname IN ('hypertable_chunk_local_size', 'compressed_chunk_stats',
                        'bgw_job_stat', 'bgw_policy_chunk_stats')
ON CONFLICT DO NOTHING;

-- We can now copy back saved initprivs.
WITH to_update AS (
     SELECT objoid, tmpini
     FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
        JOIN pg_init_privs ip ON ip.objoid = cl.oid AND ip.objsubid = 0
        JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname)
UPDATE pg_init_privs
   SET initprivs = tmpini
  FROM to_update
 WHERE to_update.objoid = pg_init_privs.objoid
   AND classoid = 'pg_class'::regclass
   AND objsubid = 0;

-- Can only restore permissions on views after they have been rebuilt,
-- so we restore for all types of objects here.
WITH to_update AS (
     SELECT cl.oid, tmpacl
     FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
                      JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname)
UPDATE pg_class cl SET relacl = tmpacl
  FROM to_update WHERE cl.oid = to_update.oid;

DROP TABLE _timescaledb_internal.saved_privs;

-- warn about partial storage format change for numeric
DO $$
DECLARE
  cagg_name text;
  cagg_column text;
  cnt int := 0;
BEGIN
  IF current_setting('server_version_num')::int <  140000 THEN
    FOR cagg_name, cagg_column IN
      SELECT
        attrelid::regclass::text,
        att.attname
      FROM _timescaledb_catalog.continuous_agg cagg
      INNER JOIN pg_attribute att ON (
        att.attrelid = format('%I.%I',cagg.user_view_schema,cagg.user_view_name)::regclass AND
        atttypid = 'numeric'::regtype)
      WHERE cagg.finalized = false
    LOOP
      RAISE WARNING 'Continuous Aggregate: % column: %', cagg_name, cagg_column;
      cnt := cnt + 1;
    END LOOP;
    IF cnt > 0 THEN
      RAISE WARNING 'The aggregation state format for numeric changed between PG13 and PG14. You should upgrade the above mentioned caggs to the new format before upgrading to PG14';
    END IF;
  END IF;
END $$;


-- Create dimension partition information for existing space-partitioned hypertables
CREATE FUNCTION _timescaledb_internal.update_dimension_partition(hypertable REGCLASS) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_partition_update' LANGUAGE C VOLATILE;
SELECT _timescaledb_internal.update_dimension_partition(format('%I.%I', h.schema_name, h.table_name))
FROM _timescaledb_catalog.hypertable h
INNER JOIN _timescaledb_catalog.dimension d ON (d.hypertable_id = h.id)
WHERE d.interval_length IS NULL;
DROP FUNCTION _timescaledb_internal.update_dimension_partition;

-- Report warning when partial aggregates are used
DO $$
DECLARE
  cagg_name text;
BEGIN
    FOR cagg_name IN
      SELECT
        format('%I.%I', user_view_schema, user_view_name)
      FROM _timescaledb_catalog.continuous_agg
      WHERE finalized IS FALSE
      ORDER BY 1
    LOOP
      RAISE WARNING 'Continuous Aggregate: % with old format will not be supported with PG15. You should upgrade to the new format', cagg_name;
    END LOOP;
END $$;
