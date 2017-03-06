\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/insert_two_partitions.sql
\o

\set ECHO ALL


\c postgres

\! pg_dump -h localhost -U postgres -Fc single > dump/single.sql
\! dropdb -h localhost -U postgres single

\! pg_restore -h localhost -U postgres -d postgres -C dump/single.sql

\c single
SELECT * FROM "testNs";

--query for the extension tables/sequences that will not be dumped by pg_dump (should be empty)
SELECT objid::regclass, *
FROM pg_catalog.pg_depend
WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND
        refobjid = (select oid from pg_extension where extname='timescaledb') AND
        deptype = 'e' AND
        classid='pg_catalog.pg_class'::pg_catalog.regclass
        AND objid NOT IN (select unnest(extconfig) from pg_extension where extname='timescaledb')
