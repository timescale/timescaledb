-- Add the missing NOT NULL constraint, which also propagates it to the chunks.
DO $$
DECLARE
  mat_ht regclass;
  colname name;
BEGIN
  FOR mat_ht, colname IN
    SELECT a.attrelid::regclass, a.attname
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
    JOIN pg_catalog.pg_attribute a
      ON a.attrelid = pg_catalog.format('%I.%I', h.schema_name, h.table_name)::regclass
    WHERE pg_catalog.current_setting('server_version_num')::int >= 180000
      AND a.attnum > 0
      AND a.attnotnull
      AND NOT EXISTS (
        SELECT FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = a.attrelid AND c.contype = 'n' AND c.conkey = ARRAY[a.attnum])
  LOOP
    -- The column cannot contain NULLs, so add the constraint as NOT VALID
    -- to avoid scanning the chunks and mark it as valid in the catalog.
    EXECUTE pg_catalog.format('ALTER TABLE %s ADD NOT NULL %I NOT VALID', mat_ht, colname);

    UPDATE pg_catalog.pg_constraint SET convalidated = true
    WHERE contype = 'n'
      AND NOT convalidated
      AND (conrelid = mat_ht OR conrelid IN (
        SELECT inhrelid FROM pg_catalog.pg_inherits WHERE inhparent = mat_ht));
  END LOOP;
END
$$;

DROP FUNCTION IF EXISTS _timescaledb_functions.get_segmentby_defaults(regclass);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_orderby_defaults(regclass, text[]);
