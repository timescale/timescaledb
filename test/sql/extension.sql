\ir include/create_single_db.sql

\c single

--list all extension functions in public schema
SELECT DISTINCT proname
FROM pg_proc
WHERE OID IN (
    SELECT objid
    FROM pg_catalog.pg_depend                                                                                                               WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND
    refobjid = (select oid from pg_extension where extname='timescaledb') AND
    deptype = 'e' and classid = 'pg_catalog.pg_proc'::regclass
) AND pronamespace = 'public'::regnamespace
ORDER BY proname;
