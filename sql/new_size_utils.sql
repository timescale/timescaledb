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
      AND relkind = 'r' 
   LEFT OUTER JOIN
      _timescaledb_catalog.compression_chunk_size map 
      ON map.chunk_id = c.id ;

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
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- heap_bytes        - Disk space used by main_table (like pg_relation_size(main_table))
-- index_bytes        - Disk space used by indexes
-- toast_bytes        - Disk space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data

CREATE OR REPLACE FUNCTION hypertable_detailed_size(
    main_table              REGCLASS
)
RETURNS TABLE (heap_bytes BIGINT,
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

-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_size         - Pretty output of heap_bytes
-- index_bytes        - Pretty output of index_bytes
-- toast_bytes        - Pretty output of toast_bytes
-- total_size         - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION hypertable_detailed_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (heap_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT,
               node_name   NAME) LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        RETURN QUERY
        SELECT pg_size_pretty(heap_bytes) as table_size,
               pg_size_pretty(index_bytes) as index_size,
               pg_size_pretty(toast_bytes) as toast_size,
               pg_size_pretty(total_bytes) as total_size,
               node_name
               FROM @extschema@.hypertable_detailed_size(main_table);

END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_local_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_name  NAME,
    heap_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint)
LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
BEGIN
   RETURN QUERY
   SELECT
      ch.chunk_id,
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

---should return same information as chunk_local_size--
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_name  NAME,
    heap_bytes bigint,
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
-- heap_bytes                   - Disk space used by main_table
-- index_bytes                   - Disk space used by indexes
-- toast_bytes                   - Disk space of toast tables
-- total_bytes                   - Disk space used in total

CREATE OR REPLACE FUNCTION chunk_detailed_size(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table NAME,
               heap_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               data_node   NAME)
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
            RETURN QUERY SELECT * FROM _timescaledb_internal.chunk_remote_size(schema_name, table_name);
        ELSE
            RETURN QUERY SELECT *, NULL::NAME FROM _timescaledb_internal.chunk_local_size(schema_name, table_name);
        END CASE;
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
-- table_size                    - Pretty output of heap_bytes
-- index_size                    - Pretty output of index_bytes
-- toast_size                    - Pretty output of toast_bytes
-- total_size                    - Pretty output of total_bytes


CREATE OR REPLACE FUNCTION chunk_detailed_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT,
               node_name TEXT
               )
               LANGUAGE PLPGSQL STABLE STRICT
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        RETURN QUERY
        SELECT pg_size_pretty(heap_bytes) as table_size,
               pg_size_pretty(index_bytes) as index_size,
               pg_size_pretty(toast_bytes) as toast_size,
               pg_size_pretty(total_bytes) as total_size,
               node_name
               FROM @extschema@.chunk_detailed_size(main_table);
END;
$BODY$;
