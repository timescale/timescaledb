\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/insert.sql
\o

\set ECHO ALL


\c postgres

\! pg_dump -h localhost -U postgres -Fc Test1 > dump/Test1.sql
\! pg_dump -h localhost -U postgres -Fc test2 > dump/test2.sql
\! pg_dump -h localhost -U postgres -Fc meta  > dump/meta.sql

\! dropdb -h localhost -U postgres Test1
\! dropdb -h localhost -U postgres test2
\! dropdb -h localhost -U postgres meta

\! pg_restore -h localhost -U postgres -d postgres -C dump/Test1.sql
\! pg_restore -h localhost -U postgres -d postgres -C dump/test2.sql
\! pg_restore -h localhost -U postgres -d postgres -C dump/meta.sql

\c test2
SELECT * FROM "testNs";

--query for the extension tables/sequences that will not be dumped by pg_dump (should be empty)
SELECT objid::regclass, *
FROM pg_catalog.pg_depend
WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND 
        refobjid = (select oid from pg_extension where extname='iobeamdb') AND 
        deptype = 'e' AND 
        classid='pg_catalog.pg_class'::pg_catalog.regclass
        AND objid NOT IN (select unnest(extconfig) from pg_extension where extname='iobeamdb')




