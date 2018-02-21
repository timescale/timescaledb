\d+ _timescaledb_catalog.hypertable
\d+ _timescaledb_catalog.chunk
\d+ _timescaledb_catalog.dimension
\d+ _timescaledb_catalog.dimension_slice
\d+ _timescaledb_catalog.chunk_constraint
\d+ _timescaledb_catalog.chunk_index
\d+ _timescaledb_catalog.tablespace

\di+ _timescaledb_catalog.*
-- Do not list sequence details because of potentially different state
-- of the sequence between updated and restored versions of a database
\ds _timescaledb_catalog.*;
\df _timescaledb_internal.*;
\df+ _timescaledb_internal.*;
\df public.*;
\df+ public.*;

\dy
\d+ PUBLIC.*

\dx+ timescaledb
SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

-- The list of tables configured to be dumped.
SELECT obj::regclass::text
FROM (SELECT unnest(extconfig) AS obj FROM pg_extension WHERE extname='timescaledb') AS objects
ORDER BY obj::regclass::text;

SELECT * FROM _timescaledb_catalog.chunk_constraint ORDER BY chunk_id, dimension_slice_id, constraint_name;
SELECT index_name FROM _timescaledb_catalog.chunk_index ORDER BY index_name;

\d+ _timescaledb_internal._hyper*

-- INSERT data to create a new chunk after update or restore.
INSERT INTO devices(id,floor) VALUES
('dev5', 5);
INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1, series_2) VALUES
(1258894000000000000, 'dev5', 2.2, 1, 2);

SELECT * FROM public."two_Partitions";

\d+ _timescaledb_internal.*

CREATE OR REPLACE FUNCTION timescaledb_integrity_test()
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    constraint_row RECORD;
    index_row      RECORD;
    chunk_count    INTEGER;
    chunk_constraint_count INTEGER;
    chunk_index_count      INTEGER;
BEGIN
    -- Check integrity of chunk indexes
    FOR index_row IN
    SELECT h.schema_name, c.relname AS index_name, h.id AS hypertable_id, h.table_name AS hypertable_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN pg_index i ON (i.indrelid = format('%I.%I', h.schema_name, h.table_name)::regclass)
    INNER JOIN pg_class c ON (i.indexrelid = c.oid)
    EXCEPT
    SELECT h.schema_name, c.relname AS index_name, h.id AS hypertable_id, h.table_name AS hypertable_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN pg_index i ON (i.indrelid = format('%I.%I', h.schema_name, h.table_name)::regclass)
    INNER JOIN pg_class c ON (i.indexrelid = c.oid)
    INNER JOIN pg_constraint cc ON (c.oid = cc.conindid)
    LOOP
        SELECT count(*) FROM _timescaledb_catalog.chunk c
        WHERE c.hypertable_id = index_row.hypertable_id
        INTO STRICT chunk_count;

        SELECT count(c.*) FROM _timescaledb_catalog.chunk_index c
        WHERE c.hypertable_id = index_row.hypertable_id
        AND c.hypertable_index_name = index_row.index_name
        INTO STRICT chunk_index_count;

        IF chunk_index_count != chunk_count THEN
           RAISE EXCEPTION 'Missing chunk indexes. Expected %, but found %', chunk_count, chunk_index_count;
        END IF;
    END LOOP;

    -- Check integrity of chunk_constraints
    FOR constraint_row IN
    SELECT c.conname, h.id AS hypertable_id FROM _timescaledb_catalog.hypertable h INNER JOIN
           pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass)
        WHERE c.contype != 'c'
    LOOP
        SELECT count(*) FROM _timescaledb_catalog.chunk c
        WHERE c.hypertable_id = constraint_row.hypertable_id
        INTO STRICT chunk_count;

        SELECT count(cc.*) FROM _timescaledb_catalog.chunk_constraint cc,
        _timescaledb_catalog.chunk c
        WHERE hypertable_constraint_name = constraint_row.conname
        AND c.id = cc.chunk_id
        AND c.hypertable_id = constraint_row.hypertable_id
        INTO STRICT chunk_constraint_count;

        IF chunk_constraint_count != chunk_count THEN
           RAISE EXCEPTION 'Missing chunk constraints for %. Expected %, but found %', constraint_row.conname, chunk_count, chunk_constraint_count;
        END IF;
    END LOOP;
END;
$BODY$;

SELECT timescaledb_integrity_test();
