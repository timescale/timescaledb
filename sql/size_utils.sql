
-- This file contains utility functions to get the relation size
-- of hypertables, chunks, and indexes on hypertables.

-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_bytes        - Disk space used by main_table (like pg_relation_size(main_table))
-- index_bytes        - Disc space used by indexes
-- toast_bytes        - Disc space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data
-- table_size         - Pretty output of table_bytes
-- index_bytes        - Pretty output of index_bytes
-- toast_bytes        - Pretty output of toast_bytes
-- total_size         - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION hypertable_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT) LANGUAGE PLPGSQL VOLATILE
               SECURITY DEFINER SET search_path = ''
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$
        SELECT table_bytes,
               index_bytes,
               toast_bytes,
               total_bytes,
               pg_size_pretty(table_bytes) as table,
               pg_size_pretty(index_bytes) as index,
               pg_size_pretty(toast_bytes) as toast,
               pg_size_pretty(total_bytes) as total
               FROM (
               SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
                      SELECT
                      sum(pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"'))::bigint as total_bytes,
                      sum(pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"'))::bigint AS index_bytes,
                      sum(pg_total_relation_size(reltoastrelid))::bigint AS toast_bytes
                      FROM
                      _timescaledb_catalog.hypertable h,
                      _timescaledb_catalog.chunk c,
                      pg_class pgc,
                      pg_namespace pns
                      WHERE h.schema_name = %L
                      AND h.table_name = %L
                      AND c.hypertable_id = h.id
                      AND pgc.relname = h.table_name
                      AND pns.oid = pgc.relnamespace
                      AND pns.nspname = h.schema_name
                      AND relkind = 'r'
                      ) sub1
               ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;

-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id          - timescaledb id of a chunk
-- chunk_table       - table used for the chunk
-- table_bytes       - Disk space used by main_table
-- index_bytes       - Disk space used by indexes
-- toast_bytes       - Disc space of toast tables
-- total_bytes       - Disk space used in total
-- table_size        - Pretty output of table_bytes
-- index_size        - Pretty output of index_bytes
-- toast_size        - Pretty output of toast_bytes
-- total_size        - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION chunk_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               dimensions NAME[],
               ranges int8range[],
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT)
               LANGUAGE PLPGSQL VOLATILE
               SECURITY DEFINER SET search_path = ''
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$

        SELECT chunk_id,
        chunk_table,
        dimensions,
        ranges,
        table_bytes,
        index_bytes,
        toast_bytes,
        total_bytes,
        pg_size_pretty(table_bytes) AS table,
        pg_size_pretty(index_bytes) AS index,
        pg_size_pretty(toast_bytes) AS toast,
        pg_size_pretty(total_bytes) AS total
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               '"' || c.schema_name || '"."' || c.table_name || '"' as chunk_table,
               pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"') AS total_bytes,
               pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"') AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name) as dimensions,
               array_agg(int8range(range_start, range_end)) as ranges
               FROM
               _timescaledb_catalog.hypertable h,
               _timescaledb_catalog.chunk c,
               _timescaledb_catalog.chunk_constraint cc,
               _timescaledb_catalog.dimension d,
               _timescaledb_catalog.dimension_slice ds,
               pg_class pgc,
               pg_namespace pns
               WHERE h.schema_name = %L
                     AND h.table_name = %L
                     AND pgc.relname = h.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = h.schema_name
                     AND relkind = 'r'
                     AND c.hypertable_id = h.id
                     AND c.id = cc.chunk_id
                     AND cc.dimension_slice_id = ds.id
                     AND ds.dimension_id = d.id
  
               GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
               ) sub1
        ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;

-- Get sizes of indexes on a hypertable
--
-- main_table - hypertable to get index sizes of
--
-- Returns:
-- index_name           - index on hyper table
-- total_bytes          - size of index on disk
-- total_size           - pretty output of total_bytes

CREATE OR REPLACE FUNCTION indexes_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (index_name TEXT,
               total_bytes BIGINT,
               total_size  TEXT) LANGUAGE PLPGSQL VOLATILE
               SECURITY DEFINER SET search_path = ''
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$
        SELECT hi.main_schema_name || '.' || hi.main_index_name,
               sum(pg_relation_size('"' || ci.schema_name || '"."' || ci.index_name || '"'))::bigint,
               pg_size_pretty(sum(pg_relation_size('"' || ci.schema_name || '"."' || ci.index_name || '"')))
        FROM
        _timescaledb_catalog.hypertable h,
        _timescaledb_catalog.hypertable_index hi,
        _timescaledb_catalog.chunk_index ci
        WHERE h.id = hi.hypertable_id
              AND h.schema_name = %L
              AND h.table_name = %L
              AND ci.main_index_name = hi.main_index_name
              AND ci.main_schema_name = hi.main_schema_name
        GROUP BY hi.main_schema_name || '.' || hi.main_index_name;
        $$,
        schema_name, table_name);

END;
$BODY$;
