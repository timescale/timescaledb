-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utility functions to get the relation size
-- of hypertables, chunks, and indexes on hypertables.

CREATE OR REPLACE VIEW _timescaledb_internal.hypertable_chunk_local_size AS 
SELECT
   h.schema_name,
   h.table_name,
   h.id as hypertable_id,
   c.id as chunk_id,
   c.schema_name as chunk_schema,
   c.table_name as chunk_name,
   pg_total_relation_size(format('%I.%I', c.schema_name, c.table_name))::bigint AS total_bytes,
   pg_indexes_size(format('%I.%I', c.schema_name, c.table_name))::bigint AS index_bytes,
   pg_total_relation_size(reltoastrelid)::bigint AS toast_bytes,
   map.compressed_heap_size,
   map.compressed_index_size,
   map.compressed_toast_size 
FROM
   _timescaledb_catalog.hypertable h 
   INNER JOIN
      _timescaledb_catalog.chunk c 
      ON h.id = c.hypertable_id 
      and c.dropped = false 
   INNER JOIN
      pg_class pgc 
      ON pgc.relname = h.table_name 
   INNER JOIN
      pg_namespace pns 
      ON pns.oid = pgc.relnamespace 
      AND pns.nspname = h.schema_name 
   LEFT OUTER JOIN
      _timescaledb_catalog.compression_chunk_size map 
      ON map.chunk_id = c.id 
WHERE pgc.relkind = 'r';

GRANT SELECT ON  _timescaledb_internal.hypertable_chunk_local_size TO PUBLIC;
 
CREATE OR REPLACE FUNCTION _timescaledb_internal.data_node_hypertable_info(
    node_name              NAME,
    schema_name_in name,
    table_name_in name
)
RETURNS TABLE (
    table_bytes     bigint,
    index_bytes     bigint,
    toast_bytes     bigint,
    total_bytes     bigint)
AS '@MODULE_PATHNAME@', 'ts_dist_remote_hypertable_info' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.data_node_chunk_info(
    node_name              NAME,
    schema_name_in name,
    table_name_in name
)
RETURNS TABLE (
    chunk_id        integer,
    chunk_schema    name,
    chunk_name      name,
    table_bytes     bigint,
    index_bytes     bigint,
    toast_bytes     bigint,
    total_bytes     bigint)
AS '@MODULE_PATHNAME@', 'ts_dist_remote_chunk_info' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_local_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint)
LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
BEGIN
   RETURN QUERY 
   SELECT
      (sub2.table_bytes + sub2.compressed_heap_bytes)::bigint as heap_bytes,
      (sub2.index_bytes + sub2.compressed_index_bytes)::bigint as index_bytes,
      (sub2.toast_bytes + sub2.compressed_toast_bytes)::bigint as toast_bytes,
      (sub2.total_bytes + sub2.compressed_heap_bytes + sub2.compressed_index_bytes + sub2.compressed_toast_bytes)::bigint as total_bytes 
   FROM
      (
         SELECT
            *,
            sub1.total_bytes - sub1.index_bytes - sub1.toast_bytes AS table_bytes 
         FROM
            (
               SELECT
                  sum(ch.total_bytes) as total_bytes,
                  COALESCE( sum(ch.index_bytes) , 0 ) as index_bytes,
                  COALESCE( sum(ch.toast_bytes), 0 ) as toast_bytes,
                  COALESCE( sum(ch.compressed_heap_size) , 0 ) as compressed_heap_bytes,
                  COALESCE( sum(ch.compressed_index_size) , 0) as compressed_index_bytes,
                  COALESCE( sum(ch.compressed_toast_size) , 0 ) as compressed_toast_bytes 
               FROM
                  _timescaledb_internal.hypertable_chunk_local_size ch 
               WHERE
                  schema_name = schema_name_in 
                  AND table_name = table_name_in 
               GROUP BY
                  hypertable_id 
            ) sub1 
      ) sub2;
END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint,
    node_name   NAME)
LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT
        sum(entry.table_bytes)::bigint AS table_bytes,
        sum(entry.index_bytes)::bigint AS index_bytes,
        sum(entry.toast_bytes)::bigint AS toast_bytes,
        sum(entry.total_bytes)::bigint AS total_bytes,
        srv.node_name
    FROM (
        SELECT
            s.node_name,
            _timescaledb_internal.ping_data_node (s.node_name) AS node_up
        FROM
            _timescaledb_catalog.hypertable AS ht,
            _timescaledb_catalog.hypertable_data_node AS s
        WHERE
            ht.schema_name = schema_name_in
            AND ht.table_name = table_name_in
            AND s.hypertable_id = ht.id
         ) AS srv
    LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_hypertable_info(
    CASE WHEN srv.node_up THEN
        srv.node_name
    ELSE
        NULL
    END, schema_name_in, table_name_in) entry ON TRUE
    GROUP BY srv.node_name;
END;
$BODY$;

-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_bytes        - Disk space used by main_table (like pg_relation_size(main_table))
-- index_bytes        - Disk space used by indexes
-- toast_bytes        - Disk space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data

CREATE OR REPLACE FUNCTION hypertable_detailed_size(
    main_table              REGCLASS
)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME
               ) LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
        is_distributed   BOOL;
BEGIN
        SELECT relname, nspname, replication_factor > 0
        INTO STRICT table_name, schema_name, is_distributed
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname AND ht.table_name = c.relname)
        WHERE c.OID = main_table;

        CASE WHEN is_distributed THEN
            RETURN QUERY SELECT * FROM _timescaledb_internal.hypertable_remote_size(schema_name, table_name);
        ELSE
            RETURN QUERY SELECT *, NULL::name FROM _timescaledb_internal.hypertable_local_size(schema_name, table_name);
        END CASE;
END;
$BODY$;

--- returns total-bytes for a hypertable (includes table + index)
CREATE OR REPLACE FUNCTION hypertable_size(
    main_table              REGCLASS
)
RETURNS BIGINT 
LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
  num_bytes BIGINT;
BEGIN
   SELECT sum(hd.total_bytes) INTO STRICT num_bytes
   FROM hypertable_detailed_size(main_table) hd;
   RETURN num_bytes;
END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_local_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_schema NAME,
    chunk_name  NAME,
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint)
LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
BEGIN
   RETURN QUERY
   SELECT
      ch.chunk_id,
      ch.chunk_schema,
      ch.chunk_name,
      (ch.total_bytes - COALESCE( ch.index_bytes , 0 ) - COALESCE( ch.toast_bytes, 0 ) + COALESCE( ch.compressed_heap_size , 0 ))::bigint  as heap_bytes,
      (COALESCE( ch.index_bytes, 0 ) + COALESCE( ch.compressed_index_size , 0) )::bigint as index_bytes,
      (COALESCE( ch.toast_bytes, 0 ) + COALESCE( ch.compressed_toast_size, 0 ))::bigint as toast_bytes,
      (ch.total_bytes + COALESCE( ch.compressed_heap_size, 0 ) + COALESCE( ch.compressed_index_size, 0) + COALESCE( ch.compressed_toast_size, 0 ))::bigint as total_bytes 
   FROM
   _timescaledb_internal.hypertable_chunk_local_size ch 
   WHERE
      ch.schema_name = schema_name_in 
      AND ch.table_name = table_name_in 
; 
END;
$BODY$;

---should return same information as chunks_local_size--
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_schema NAME,
    chunk_name  NAME,
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint,
    node_name NAME)
LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
BEGIN
    RETURN QUERY
    SELECT
        entry.chunk_id,
        entry.chunk_schema,
        entry.chunk_name,
        entry.table_bytes AS table_bytes,
        entry.index_bytes AS index_bytes,
        entry.toast_bytes AS toast_bytes,
        entry.total_bytes AS total_bytes,
        srv.node_name
    FROM (
        SELECT
            s.node_name,
            _timescaledb_internal.ping_data_node (s.node_name) AS node_up
        FROM
            _timescaledb_catalog.hypertable AS ht,
            _timescaledb_catalog.hypertable_data_node AS s
        WHERE
            ht.schema_name = schema_name_in
            AND ht.table_name = table_name_in
            AND s.hypertable_id = ht.id
         ) AS srv
    LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_chunk_info(
    CASE WHEN srv.node_up THEN
        srv.node_name
    ELSE
        NULL
    END , schema_name_in, table_name_in) entry ON TRUE;
END;
$BODY$;

-- Get relation size of the chunks of an hypertable
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_schema                  - schema name for chunk
-- chunk_name                    - chunk table name
-- table_bytes                   - Disk space used by chunk table 
-- index_bytes                   - Disk space used by indexes
-- toast_bytes                   - Disk space of toast tables
-- total_bytes                   - Disk space used in total
-- node_name                     - node on which chunk lives if this is
--                              a distributed hypertable.
CREATE OR REPLACE FUNCTION chunks_detailed_size(
    main_table              REGCLASS
)
RETURNS TABLE (
               chunk_schema NAME,
               chunk_name NAME,
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME)
               LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
        is_distributed   BOOL;
BEGIN
        SELECT relname, nspname, replication_factor > 0
        INTO STRICT table_name, schema_name, is_distributed
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname AND ht.table_name = c.relname)
        WHERE c.OID = main_table;

        CASE WHEN is_distributed THEN
            RETURN QUERY SELECT ch.chunk_schema, ch.chunk_name, ch.table_bytes, ch.index_bytes, 
                        ch.toast_bytes, ch.total_bytes, ch.node_name   
            FROM _timescaledb_internal.chunks_remote_size(schema_name, table_name) ch;
        ELSE
            RETURN QUERY SELECT chl.chunk_schema, chl.chunk_name, chl.table_bytes, chl.index_bytes, 
                        chl.toast_bytes, chl.total_bytes, NULL::NAME   
            FROM _timescaledb_internal.chunks_local_size(schema_name, table_name) chl;
        END CASE;
END;
$BODY$;

-- Get sizes of indexes on a hypertable
--
-- main_table - hypertable to get index sizes of
--
-- Returns:
-- index_name           - index on hyper table
-- total_bytes          - size of index on disk

CREATE OR REPLACE FUNCTION indexes_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (index_name TEXT,
               total_bytes BIGINT)
               LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
<<main>>
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY
        SELECT format('%I.%I', h.schema_name, ci.hypertable_index_name),
               sum(pg_relation_size(c.oid))::bigint
        FROM
        pg_class c,
        pg_namespace n,
        _timescaledb_catalog.hypertable h,
        _timescaledb_catalog.chunk ch,
        _timescaledb_catalog.chunk_index ci
        WHERE ch.schema_name = n.nspname
            AND c.relnamespace = n.oid
            AND c.relname = ci.index_name
            AND ch.id = ci.chunk_id
            AND h.id = ci.hypertable_id
            AND h.schema_name = main.schema_name
            AND h.table_name = main.table_name
        GROUP BY h.schema_name, ci.hypertable_index_name;
END;
$BODY$;

------- REMOVE chunk_relation_size -----
-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id                      - Timescaledb id of a chunk
-- chunk_table                   - Table used for the chunk
-- partitioning_columns          - Partitioning column names
-- partitioning_column_types     - Type of partitioning columns
-- partitioning_hash_functions   - Hash functions of partitioning columns
-- ranges                        - Partition ranges for each dimension of the chunk
-- table_bytes                   - Disk space used by main_table
-- index_bytes                   - Disk space used by indexes
-- toast_bytes                   - Disk space of toast tables
-- total_bytes                   - Disk space used in total

CREATE OR REPLACE FUNCTION chunk_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               partitioning_columns NAME[],
               partitioning_column_types REGTYPE[],
               partitioning_hash_functions TEXT[],
               ranges int8range[],
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT)
               LANGUAGE PLPGSQL STABLE STRICT
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
        partitioning_columns,
        partitioning_column_types,
        partitioning_hash_functions,
        ranges,
        table_bytes,
        index_bytes,
        toast_bytes,
        total_bytes
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               format('%%I.%%I', c.schema_name, c.table_name) as chunk_table,
               pg_total_relation_size(format('%%I.%%I', c.schema_name, c.table_name)) AS total_bytes,
               pg_indexes_size(format('%%I.%%I', c.schema_name, c.table_name)) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
               array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
               array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_hash_functions,
               array_agg(int8range(range_start, range_end) ORDER BY d.interval_length, d.column_name ASC) as ranges
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
                     AND pgc.relname = c.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = c.schema_name
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

-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id                      - Timescaledb id of a chunk
-- chunk_table                   - Table used for the chunk
-- partitioning_columns          - Partitioning column names
-- partitioning_column_types     - Type of partitioning columns
-- partitioning_hash_functions   - Hash functions of partitioning columns
-- ranges                        - Partition ranges for each dimension of the chunk
-- table_size                    - Pretty output of table_bytes
-- index_size                    - Pretty output of index_bytes
-- toast_size                    - Pretty output of toast_bytes
-- total_size                    - Pretty output of total_bytes


CREATE OR REPLACE FUNCTION chunk_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               partitioning_columns NAME[],
               partitioning_column_types REGTYPE[],
               partitioning_hash_functions TEXT[],
               ranges TEXT[],
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT
               )
               LANGUAGE PLPGSQL STABLE STRICT
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
        partitioning_columns,
        partitioning_column_types,
        partitioning_functions,
        ranges,
        pg_size_pretty(table_bytes) AS table,
        pg_size_pretty(index_bytes) AS index,
        pg_size_pretty(toast_bytes) AS toast,
        pg_size_pretty(total_bytes) AS total
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               format('%%I.%%I', c.schema_name, c.table_name) as chunk_table,
               pg_total_relation_size(format('%%I.%%I', c.schema_name, c.table_name)) AS total_bytes,
               pg_indexes_size(format('%%I.%%I', c.schema_name, c.table_name)) AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
               array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
               array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_functions,
               array_agg('[' || _timescaledb_internal.range_value_to_pretty(range_start, column_type) ||
                         ',' ||
                         _timescaledb_internal.range_value_to_pretty(range_end, column_type) || ')' ORDER BY d.interval_length, d.column_name ASC) as ranges
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
                     AND pgc.relname = c.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = c.schema_name
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

CREATE OR REPLACE FUNCTION _timescaledb_internal.range_value_to_pretty(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF NOT _timescaledb_internal.dimension_is_finite(time_value) THEN
        RETURN '';
    END IF;
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('%1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
      ELSE
        RETURN time_value;
    END CASE;
END
$BODY$;

-- Convenience function to return approximate row count
--
-- main_table - hypertable to get approximate row count for; if NULL, get count
--              for all hypertables
--
-- Returns:
-- schema_name      - Schema name of the hypertable
-- table_name       - Table name of the hypertable
-- row_estimate     - Estimated number of rows according to catalog tables
CREATE OR REPLACE FUNCTION hypertable_approximate_row_count(
    main_table REGCLASS = NULL
)
    RETURNS TABLE (schema_name NAME,
                   table_name NAME,
                   row_estimate BIGINT
                  ) LANGUAGE PLPGSQL VOLATILE
    AS
$BODY$
<<main>>
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        IF main_table IS NOT NULL THEN
            SELECT relname, nspname
            INTO STRICT table_name, schema_name
            FROM pg_class c
            INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
            WHERE c.OID = main_table;
        END IF;

-- Thanks to @fvannee on Github for providing the initial draft
-- of this query
        RETURN QUERY
        SELECT h.schema_name,
            h.table_name,
            row_estimate.row_estimate
        FROM _timescaledb_catalog.hypertable h
        CROSS JOIN LATERAL (
            SELECT sum(cl.reltuples)::BIGINT AS row_estimate
            FROM _timescaledb_catalog.chunk c
            JOIN pg_class cl ON cl.relname = c.table_name
            WHERE c.hypertable_id = h.id
            GROUP BY h.schema_name, h.table_name
        ) row_estimate
        WHERE (main.table_name IS NULL OR h.table_name = main.table_name)
        AND (main.schema_name IS NULL OR h.schema_name = main.schema_name)
        ORDER BY h.schema_name, h.table_name;
END
$BODY$;
