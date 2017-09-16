\o /dev/null
\ir include/insert_two_partitions.sql
\o

-- Test that we can restore constraints
ALTER TABLE PUBLIC."two_Partitions"
ADD CONSTRAINT timeCustom_device_id_series_2_key
UNIQUE ("timeCustom", device_id, series_2);

-- Test that we can restore triggers
CREATE OR REPLACE FUNCTION test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN NEW;
END
$BODY$;

CREATE TRIGGER restore_trigger BEFORE INSERT ON PUBLIC."two_Partitions"
FOR EACH ROW EXECUTE PROCEDURE test_trigger();

SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

\d+ _timescaledb_internal._hyper_1_1_chunk
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id;
SELECT * FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY "timeCustom", device_id;
SELECT * FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY "timeCustom", device_id;

\c postgres

\! pg_dump -h localhost -U postgres -Fc single > dump/single.sql
\! dropdb -h localhost -U postgres single
\! createdb -h localhost -U postgres single
ALTER DATABASE single SET timescaledb.restoring='on';
\! pg_restore -h localhost -U postgres -d single dump/single.sql
\c single
SELECT restore_timescaledb();
ALTER DATABASE single SET timescaledb.restoring='off';

--should be same as count above
SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

--chunk schema should be the same
\d+ _timescaledb_internal._hyper_1_1_chunk
--data should be the same
SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id;
SELECT * FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY "timeCustom", device_id;
SELECT * FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY "timeCustom", device_id;

--check simple ddl still works
ALTER TABLE "two_Partitions" ADD COLUMN series_3 integer;
INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1, series_3) VALUES
(1357894000000000000, 'dev5', 1.5, 2, 4);

--query for the extension tables/sequences that will not be dumped by pg_dump (should be empty except for views)
SELECT objid::regclass
FROM pg_catalog.pg_depend
WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND
        refobjid = (select oid from pg_extension where extname='timescaledb') AND
        deptype = 'e' AND
        classid='pg_catalog.pg_class'::pg_catalog.regclass
        AND objid NOT IN (select unnest(extconfig) from pg_extension where extname='timescaledb')
