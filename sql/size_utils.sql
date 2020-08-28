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
LANGUAGE PLPGSQL VOLATILE STRICT AS
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
LANGUAGE PLPGSQL VOLATILE STRICT AS
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
-- hypertable - hypertable to get size of
--
-- Returns:
-- table_bytes        - Disk space used by hypertable (like pg_relation_size(hypertable))
-- index_bytes        - Disk space used by indexes
-- toast_bytes        - Disk space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data

CREATE OR REPLACE FUNCTION hypertable_detailed_size(
    hypertable              REGCLASS
)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME
               ) 
LANGUAGE PLPGSQL VOLATILE STRICT AS
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
        WHERE c.OID = hypertable;

        CASE WHEN is_distributed THEN
            RETURN QUERY SELECT * FROM _timescaledb_internal.hypertable_remote_size(schema_name, table_name);
        ELSE
            RETURN QUERY SELECT *, NULL::name FROM _timescaledb_internal.hypertable_local_size(schema_name, table_name);
        END CASE;
END;
$BODY$;

--- returns total-bytes for a hypertable (includes table + index)
CREATE OR REPLACE FUNCTION hypertable_size(
    hypertable              REGCLASS
)
RETURNS BIGINT 
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
  num_bytes BIGINT;
BEGIN
   SELECT sum(hd.total_bytes) INTO STRICT num_bytes
   FROM hypertable_detailed_size(hypertable) hd;
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
LANGUAGE PLPGSQL VOLATILE STRICT AS
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
LANGUAGE PLPGSQL VOLATILE STRICT AS
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
-- hypertable - hypertable to get size of
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
    hypertable              REGCLASS
)
RETURNS TABLE (
               chunk_schema NAME,
               chunk_name NAME,
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME)
LANGUAGE PLPGSQL VOLATILE STRICT AS
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
        WHERE c.OID = hypertable;

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
---------- end of detailed size functions ------

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
      WHEN 'TIMESTAMP'::regtype THEN
        RETURN format('%L', _timescaledb_internal.to_timestamp(time_value));
      WHEN 'TIMESTAMPTZ'::regtype THEN
        RETURN format('%L', _timescaledb_internal.to_timestamptz(time_value));
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', _timescaledb_internal.to_date(time_value));
      ELSE
        RETURN time_value;
    END CASE;
END
$BODY$;

-- Convenience function to return approximate row count
--
-- relation - table or hypertable to get approximate row count for
--
-- Returns:
-- Estimated number of rows according to catalog tables
CREATE OR REPLACE FUNCTION approximate_row_count(relation REGCLASS)
RETURNS BIGINT
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
	table_name       NAME;
	schema_name      NAME;
	row_count_parent BIGINT;
	row_count        BIGINT;
BEGIN
	SELECT relname, nspname, c.reltuples::bigint
	INTO table_name, schema_name, row_count_parent
	FROM pg_class c
	INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
	WHERE c.OID = relation;

	WITH RECURSIVE inherited_id AS
	(
		SELECT i.inhrelid AS oid
		FROM pg_inherits i
		JOIN pg_class base ON i.inhparent = base.oid
		JOIN pg_namespace base_ns ON base.relnamespace = base_ns.oid
		WHERE base_ns.nspname = schema_name AND base.relname = table_name
		UNION
		SELECT i.inhrelid AS oid
		FROM pg_inherits i
		JOIN inherited_id b ON i.inhparent = b.oid
	)
	SELECT sum(child.reltuples)::bigint
	INTO row_count
	FROM inherited_id i
	JOIN pg_class child ON i.oid = child.oid
	JOIN pg_namespace child_ns ON child.relnamespace = child_ns.oid;

	IF row_count IS NULL THEN
		RETURN row_count_parent;
	END IF;
	RETURN row_count_parent + row_count;
END
$BODY$;

-------- stats related to compression ------
CREATE OR REPLACE VIEW _timescaledb_internal.compressed_chunk_stats AS
SELECT
    srcht.schema_name,
    srcht.table_name,
    srcch.schema_name AS chunk_schema,
    srcch.table_name AS chunk_name,
    CASE WHEN srcch.compressed_chunk_id IS NULL THEN
        'Uncompressed'::text
    ELSE
        'Compressed'::text
    END AS compression_status,
    map.uncompressed_heap_size,
    map.uncompressed_index_size,
    map.uncompressed_toast_size,
    map.uncompressed_heap_size + map.uncompressed_toast_size + map.uncompressed_index_size AS uncompressed_total_size,
    map.compressed_heap_size,
    map.compressed_index_size,
    map.compressed_toast_size,
    map.compressed_heap_size + map.compressed_toast_size + map.compressed_index_size AS compressed_total_size
FROM
    _timescaledb_catalog.hypertable AS srcht
    JOIN _timescaledb_catalog.chunk AS srcch ON srcht.id = srcch.hypertable_id
        AND srcht.compressed_hypertable_id IS NOT NULL
        AND srcch.dropped = FALSE
    LEFT JOIN _timescaledb_catalog.compression_chunk_size map ON srcch.id = map.chunk_id;

GRANT SELECT ON _timescaledb_internal.compressed_chunk_stats TO PUBLIC;

CREATE OR REPLACE FUNCTION _timescaledb_internal.data_node_compressed_chunk_stats (node_name name, schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint
    )
AS '@MODULE_PATHNAME@' , 'ts_dist_remote_compressed_chunk_info' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_chunk_local_stats (schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint)
    LANGUAGE PLPGSQL
    STABLE STRICT
    AS $BODY$
BEGIN
    RETURN QUERY
    SELECT
        ch.chunk_schema,
        ch.chunk_name,
        ch.compression_status,
        ch.uncompressed_heap_size,
        ch.uncompressed_index_size,
        ch.uncompressed_toast_size,
        ch.uncompressed_total_size,
        ch.compressed_heap_size,
        ch.compressed_index_size,
        ch.compressed_toast_size,
        ch.compressed_total_size
    FROM
        _timescaledb_internal.compressed_chunk_stats ch
    WHERE
        ch.schema_name = schema_name_in
        AND ch.table_name = table_name_in;
END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_chunk_remote_stats (schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE PLPGSQL
    STABLE STRICT
    AS $BODY$
BEGIN
    RETURN QUERY
    SELECT
        ch.*,
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
            AND s.hypertable_id = ht.id) AS srv
    LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_compressed_chunk_stats (
    CASE WHEN srv.node_up THEN
        srv.node_name
    ELSE
        NULL
    END, schema_name_in, table_name_in) ch ON TRUE;
END;
$BODY$;

-- Get per chunk compression statistics for a hypertable that has
-- compression enabled
CREATE OR REPLACE FUNCTION chunk_compression_stats (hypertable REGCLASS)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE PLPGSQL
    STABLE STRICT
    AS $BODY$
DECLARE
    table_name name;
    schema_name name;
    is_distributed bool;
BEGIN
    SELECT
        relname,
        nspname,
        replication_factor > 0 INTO STRICT table_name,
        schema_name,
        is_distributed
    FROM
        pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname
                AND ht.table_name = c.relname)
    WHERE
        c.OID = hypertable;
    CASE WHEN is_distributed THEN
        RETURN QUERY
        SELECT
            *
        FROM
            _timescaledb_internal.compressed_chunk_remote_stats (schema_name, table_name);
        ELSE
            RETURN QUERY
            SELECT
                *,
                NULL::name
            FROM
                _timescaledb_internal.compressed_chunk_local_stats (schema_name, table_name);
    END CASE;
END;
$BODY$;

-- Get compression statistics for a hypertable that has
-- compression enabled
CREATE OR REPLACE FUNCTION hypertable_compression_stats (hypertable REGCLASS)
    RETURNS TABLE (
        total_chunks bigint,
        number_compressed_chunks bigint,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE PLPGSQL
    STABLE STRICT
    AS $BODY$
BEGIN
    RETURN QUERY
    SELECT
        count(*) AS total_chunks,
        count(*) FILTER (WHERE ch.compression_status = 'Compressed') AS number_compressed_chunks,
        sum(ch.before_compression_table_bytes)::bigint AS before_compression_table_bytes,
        sum(ch.before_compression_index_bytes)::bigint AS before_compression_index_bytes,
        sum(ch.before_compression_toast_bytes)::bigint AS before_compression_toast_bytes,
        sum(ch.before_compression_total_bytes)::bigint AS before_compression_total_bytes,
        sum(ch.after_compression_table_bytes)::bigint AS after_compression_table_bytes,
        sum(ch.after_compression_index_bytes)::bigint AS after_compression_index_bytes,
        sum(ch.after_compression_toast_bytes)::bigint AS after_compression_toast_bytes,
        sum(ch.after_compression_total_bytes)::bigint AS after_compression_total_bytes,
        ch.node_name
    FROM
        chunk_compression_stats (hypertable) ch
    GROUP BY
        ch.node_name;
END;
$BODY$;

-------------Get index size for hypertables -------
--schema_name      - schema_name for hypertable index
-- index_name      - index on hyper table
---note that the query matches against the hypertable's schema name as
-- the input is on the hypertable index nd not the chunk index.
CREATE OR REPLACE FUNCTION _timescaledb_internal.indexes_local_size(
    schema_name_in             NAME,
    index_name_in              NAME
)
RETURNS TABLE ( hypertable_id INTEGER,
                total_bytes BIGINT ) 
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
BEGIN
        RETURN QUERY
        SELECT ci.hypertable_id, sum(pg_relation_size(c.oid))::bigint
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
            AND h.schema_name = schema_name_in 
            AND ci.hypertable_index_name = index_name_in
        GROUP BY ci.hypertable_id; 
END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.data_node_index_size (node_name name, schema_name_in name, index_name_in name)
RETURNS TABLE ( hypertable_id INTEGER, total_bytes BIGINT)
AS '@MODULE_PATHNAME@' , 'ts_dist_remote_hypertable_index_info' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.indexes_remote_size(
    schema_name_in             NAME,
    table_name_in              NAME,
    index_name_in              NAME
)
RETURNS BIGINT
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
    total_bytes BIGINT;
BEGIN
    SELECT
        sum(entry.total_bytes)::bigint AS total_bytes
    INTO total_bytes
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
    JOIN LATERAL _timescaledb_internal.data_node_index_size(
    CASE WHEN srv.node_up THEN
        srv.node_name
    ELSE
        NULL
    END, schema_name_in, index_name_in) entry ON TRUE ;
    RETURN total_bytes;
END;
$BODY$;

-- Get sizes of indexes on a hypertable
--
-- index_name           - index on hyper table
--
-- Returns:
-- total_bytes          - size of index on disk

CREATE OR REPLACE FUNCTION  hypertable_index_size(
    index_name              REGCLASS
)
RETURNS BIGINT
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        ht_index_name       NAME;
        ht_schema_name      NAME;
        ht_name      NAME;
        is_distributed   BOOL;
        ht_id INTEGER;
        index_bytes BIGINT;
BEGIN

   SELECT c.relname, cl.relname, nsp.nspname       
   INTO STRICT ht_index_name, ht_name, ht_schema_name  
   FROM pg_class c, pg_index cind, pg_class cl, pg_namespace nsp
   WHERE c.oid = cind.indexrelid AND cind.indrelid = cl.oid
         AND cl.relnamespace = nsp.oid AND c.oid = index_name;
        
   SELECT replication_factor > 0
   INTO STRICT is_distributed
   FROM _timescaledb_catalog.hypertable ht
   WHERE ht.schema_name = ht_schema_name AND ht.table_name = ht_name;

   CASE WHEN is_distributed THEN
         SELECT _timescaledb_internal.indexes_remote_size(ht_schema_name, ht_name, ht_index_name) 
         INTO index_bytes ;
   ELSE
         SELECT il.total_bytes
         INTO index_bytes
         FROM _timescaledb_internal.indexes_local_size(ht_schema_name, ht_index_name) il;
   END CASE;
   RETURN index_bytes;
END;
$BODY$;

-------------End index size for hypertables -------
