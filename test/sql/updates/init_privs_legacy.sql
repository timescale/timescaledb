-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Exercise catalogs damaged by a real update using the released scripts.
CREATE ROLE initacl_legacy_keep;
CREATE ROLE initacl_legacy_dropped;
CREATE ROLE initacl_legacy_stale;
ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO initacl_legacy_keep, initacl_legacy_dropped;
ALTER DEFAULT PRIVILEGES GRANT SELECT ON SEQUENCES TO initacl_legacy_keep;
CREATE EXTENSION timescaledb VERSION '2.30.2';
ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM initacl_legacy_keep, initacl_legacy_dropped;
ALTER DEFAULT PRIVILEGES REVOKE SELECT ON SEQUENCES FROM initacl_legacy_keep;

CREATE TABLE legacy_role AS SELECT 'initacl_legacy_dropped'::regrole::oid AS oid;
CREATE TABLE legacy_data (value integer);
INSERT INTO legacy_data VALUES (42);
GRANT SELECT ON legacy_data TO initacl_legacy_keep;
GRANT SELECT ON _timescaledb_catalog.chunk TO initacl_legacy_stale;
GRANT SELECT (id) ON _timescaledb_catalog.chunk TO initacl_legacy_keep;

-- Also record the soon-to-be-deleted role as a grantor in an initial ACL.
CREATE TABLE _timescaledb_catalog.initacl_legacy_table (value integer);
GRANT SELECT ON _timescaledb_catalog.initacl_legacy_table TO initacl_legacy_dropped WITH GRANT OPTION;
SET ROLE initacl_legacy_dropped;
GRANT SELECT ON _timescaledb_catalog.initacl_legacy_table TO initacl_legacy_keep;
RESET ROLE;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_catalog.initacl_legacy_table;
REVOKE SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog FROM initacl_legacy_dropped CASCADE;

\connect
ALTER EXTENSION timescaledb UPDATE TO '2.31.0';
DROP OWNED BY initacl_legacy_dropped;
DROP ROLE initacl_legacy_dropped;
REASSIGN OWNED BY initacl_legacy_stale TO CURRENT_USER;
DROP OWNED BY initacl_legacy_stale;

-- Retain both the damaged ACL and its expected repair. Preserve the order and
-- all privileges of entries whose grantor and grantee still exist.
CREATE TABLE legacy_privs AS
SELECT c.oid::regclass::text AS relname, c.relacl, ip.initprivs,
       ARRAY(SELECT item FROM unnest(ip.initprivs) WITH ORDINALITY a(item, ord)
                 WHERE NOT EXISTS (SELECT FROM aclexplode(ARRAY[item]) x, legacy_role r
                                   WHERE r.oid IN (x.grantee, x.grantor))
                 ORDER BY ord) AS repaired_initprivs
FROM pg_class c
JOIN pg_init_privs ip ON ip.objoid = c.oid
    AND ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
WHERE c.oid IN ('_timescaledb_catalog.chunk'::regclass,
                '_timescaledb_config.bgw_job'::regclass,
                '_timescaledb_catalog.chunk_id_seq'::regclass,
                '_timescaledb_catalog.initacl_legacy_table'::regclass);

CREATE VIEW legacy_dependencies AS
SELECT d.classid, d.objid, d.objsubid, d.refclassid, d.refobjid, d.deptype
FROM pg_shdepend d
WHERE d.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND d.classid = 'pg_class'::regclass
    AND d.objid IN (SELECT relname::regclass FROM legacy_privs);
CREATE TABLE saved_legacy_dependencies AS SELECT * FROM legacy_dependencies;

DO $$
BEGIN
    IF (SELECT count(*) FROM legacy_privs) <> 4 OR NOT EXISTS (
        SELECT FROM legacy_privs s, aclexplode(s.initprivs) a, legacy_role r
        WHERE r.oid IN (a.grantee, a.grantor)
    ) THEN
        RAISE EXCEPTION 'missing dropped-role ACL fixture';
    END IF;
    IF NOT EXISTS (
        SELECT FROM legacy_privs s, aclexplode(s.initprivs) a, legacy_role r
        WHERE r.oid = a.grantor AND a.grantee = 'initacl_legacy_keep'::regrole
    ) THEN
        RAISE EXCEPTION 'missing dropped-grantor ACL fixture';
    END IF;
    IF current_setting('server_version_num')::int >= 170000 AND NOT EXISTS (
        SELECT FROM legacy_dependencies
        WHERE deptype = 'i' AND refobjid = 'initacl_legacy_stale'::regrole
    ) THEN
        RAISE EXCEPTION 'missing stale dependency fixture';
    END IF;
    IF EXISTS (
        SELECT FROM legacy_privs s, aclexplode(s.initprivs) a
        WHERE 'initacl_legacy_stale'::regrole IN (a.grantee, a.grantor)
    ) THEN
        RAISE EXCEPTION 'stale dependency role is still in the initial ACL';
    END IF;
END
$$;

CREATE FUNCTION check_legacy_init_privs(repaired boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT FROM legacy_privs s
        JOIN pg_class c ON c.oid = s.relname::regclass
        LEFT JOIN pg_init_privs ip ON ip.objoid = c.oid
            AND ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
        WHERE s.relacl IS DISTINCT FROM c.relacl OR ip.initprivs IS DISTINCT FROM
            CASE WHEN repaired THEN s.repaired_initprivs ELSE s.initprivs END
    ) THEN
        RAISE EXCEPTION 'unexpected current or initial ACL after legacy update';
    END IF;

    -- Ownership and column dependencies must survive the repair. On rollback,
    -- restore the original state of both ordinary and initial ACL dependencies.
    IF EXISTS (
        (SELECT * FROM legacy_dependencies WHERE NOT repaired OR deptype NOT IN ('a', 'i') OR objsubid <> 0
         EXCEPT SELECT * FROM saved_legacy_dependencies WHERE NOT repaired OR deptype NOT IN ('a', 'i') OR objsubid <> 0)
        UNION ALL
        (SELECT * FROM saved_legacy_dependencies WHERE NOT repaired OR deptype NOT IN ('a', 'i') OR objsubid <> 0
         EXCEPT SELECT * FROM legacy_dependencies WHERE NOT repaired OR deptype NOT IN ('a', 'i') OR objsubid <> 0)
    ) THEN
        RAISE EXCEPTION 'unexpected change to saved dependencies';
    END IF;

    IF repaired AND EXISTS (
        SELECT FROM legacy_privs s CROSS JOIN pg_roles r
        WHERE r.rolname IN ('initacl_legacy_keep', 'initacl_legacy_stale')
            AND EXISTS (SELECT FROM legacy_dependencies d
                        WHERE d.objid = s.relname::regclass AND d.objsubid = 0
                            AND d.refclassid = 'pg_authid'::regclass
                            AND d.refobjid = r.oid AND d.deptype = 'a')
                IS DISTINCT FROM EXISTS (SELECT FROM aclexplode(s.relacl) a
                                         WHERE r.oid IN (a.grantee, a.grantor))
    ) THEN
        RAISE EXCEPTION 'legacy current ACL dependencies were not repaired';
    END IF;

    IF repaired AND current_setting('server_version_num')::int >= 170000 THEN
        IF EXISTS (
            SELECT FROM legacy_dependencies d, legacy_role r
            WHERE d.deptype = 'i'
                AND d.refobjid IN (r.oid, 'initacl_legacy_stale'::regrole)
        ) OR EXISTS (
            SELECT FROM legacy_privs s
            WHERE EXISTS (
                SELECT FROM legacy_dependencies d
                WHERE d.objid = s.relname::regclass AND d.objsubid = 0
                    AND d.refclassid = 'pg_authid'::regclass AND d.deptype = 'i'
                    AND d.refobjid = 'initacl_legacy_keep'::regrole)
                IS DISTINCT FROM EXISTS (
                    SELECT FROM aclexplode(s.repaired_initprivs) a
                    WHERE 'initacl_legacy_keep'::regrole IN (a.grantee, a.grantor))
        ) THEN
            RAISE EXCEPTION 'legacy initial privilege dependencies were not repaired';
        END IF;
    END IF;

    IF (SELECT array_agg(value) FROM legacy_data) IS DISTINCT FROM ARRAY[42]
        OR NOT has_table_privilege('initacl_legacy_keep', 'legacy_data', 'SELECT') THEN
        RAISE EXCEPTION 'legacy update changed user data or privileges';
    END IF;
END
$$;

SELECT check_legacy_init_privs(false);
\connect
BEGIN;
ALTER EXTENSION timescaledb UPDATE TO :'TO_VERSION';
SELECT check_legacy_init_privs(true);
ROLLBACK;
\connect
SELECT check_legacy_init_privs(false);
\connect
ALTER EXTENSION timescaledb UPDATE TO :'TO_VERSION';
SELECT check_legacy_init_privs(true);
REASSIGN OWNED BY initacl_legacy_stale TO CURRENT_USER;
DROP OWNED BY initacl_legacy_stale;
DROP ROLE initacl_legacy_stale;
