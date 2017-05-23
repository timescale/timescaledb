\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

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


\c single
--check simple ddl still works
ALTER TABLE "two_Partitions" ADD COLUMN series_3 integer;
INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1, series_3) VALUES
(1357894000000000000, 'dev5', 1.5, 2, 4);

SELECT * FROM "two_Partitions" order by "timeCustom", device_id;

--query for the extension tables/sequences that will not be dumped by pg_dump (should be empty except for views)
SELECT objid::regclass
FROM pg_catalog.pg_depend
WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND
        refobjid = (select oid from pg_extension where extname='timescaledb') AND
        deptype = 'e' AND
        classid='pg_catalog.pg_class'::pg_catalog.regclass
        AND objid NOT IN (select unnest(extconfig) from pg_extension where extname='timescaledb')
