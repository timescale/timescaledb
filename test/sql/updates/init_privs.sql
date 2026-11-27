-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Run in a separate database because these default privileges must apply when
-- installing the old extension, and must survive in its initial privileges.
CREATE ROLE initacl_keep;
CREATE ROLE initacl_restore;
CREATE ROLE initacl_user;
CREATE TABLE saved_init_roles AS
SELECT oid, rolname FROM pg_roles
WHERE rolname IN ('initacl_keep', 'initacl_restore', 'initacl_user');
ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO initacl_keep, initacl_restore;
ALTER DEFAULT PRIVILEGES GRANT SELECT ON SEQUENCES TO initacl_keep, initacl_restore;
CREATE EXTENSION timescaledb VERSION :'FROM_VERSION';
ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM initacl_keep, initacl_restore;
ALTER DEFAULT PRIVILEGES REVOKE SELECT ON SEQUENCES FROM initacl_keep, initacl_restore;

-- The update's GRANTs will capture initacl_user and omit initacl_restore.
-- Restoring initprivs must remove the former dependency, recreate the latter,
-- and leave initacl_keep's dependency intact.
GRANT SELECT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job
    TO initacl_user WITH GRANT OPTION;
GRANT SELECT ON SEQUENCE _timescaledb_catalog.chunk_id_seq
    TO initacl_user WITH GRANT OPTION;
REVOKE SELECT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job
    FROM initacl_restore;
REVOKE SELECT ON SEQUENCE _timescaledb_catalog.chunk_id_seq FROM initacl_restore;

CREATE TABLE saved_init_privs AS
SELECT c.oid::regclass::text AS relname, c.relacl, ip.initprivs
FROM pg_class c
JOIN pg_init_privs ip ON ip.objoid = c.oid
    AND ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
WHERE c.oid IN ('_timescaledb_catalog.chunk'::regclass,
                '_timescaledb_config.bgw_job'::regclass,
                '_timescaledb_catalog.chunk_id_seq'::regclass);

CREATE FUNCTION check_init_privs() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT count(*) FROM saved_init_privs) <> 3 THEN
        RAISE EXCEPTION 'missing initial privilege test fixtures';
    END IF;

    IF EXISTS (
        SELECT FROM saved_init_privs s
        LEFT JOIN pg_class c ON c.oid = s.relname::regclass
        LEFT JOIN pg_init_privs ip ON ip.objoid = c.oid
            AND ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
        WHERE ip.objoid IS NULL OR s.relacl IS DISTINCT FROM c.relacl
            OR s.initprivs IS DISTINCT FROM ip.initprivs
    ) THEN
        RAISE EXCEPTION 'update changed current or initial privileges';
    END IF;

    -- Keep the OIDs after DROP ROLE, and check current ACL dependencies too.
    IF EXISTS (
        SELECT FROM saved_init_privs s
        CROSS JOIN saved_init_roles r
        CROSS JOIN (VALUES ('a'), ('i')) kinds(deptype)
        LEFT JOIN pg_shdepend d ON d.objid = s.relname::regclass
            AND d.classid = 'pg_class'::regclass AND d.objsubid = 0
            AND d.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND d.refclassid = 'pg_authid'::regclass AND d.refobjid = r.oid
            AND d.deptype = kinds.deptype::"char"
        WHERE (kinds.deptype = 'a' OR current_setting('server_version_num')::int >= 170000)
            AND (d.refobjid IS NOT NULL) IS DISTINCT FROM EXISTS (
                SELECT FROM aclexplode(CASE WHEN kinds.deptype = 'a'
                                      THEN s.relacl ELSE s.initprivs END) a
                WHERE r.oid IN (a.grantee, a.grantor))
    ) THEN
        RAISE EXCEPTION 'role dependencies do not match saved ACLs';
    END IF;

    IF EXISTS (
        SELECT FROM saved_init_privs s
        CROSS JOIN pg_roles r
        WHERE r.rolname IN ('initacl_keep', 'initacl_restore')
            AND NOT EXISTS (
                SELECT FROM aclexplode(s.initprivs) a WHERE a.grantee = r.oid)
    ) THEN
        RAISE EXCEPTION 'missing initial grants to test roles';
    END IF;
END
$$;

SELECT check_init_privs();

-- Updates must be the first command in a fresh connection. Check rollback as
-- well as commit, including dependencies removed and recreated by the update.
\connect
BEGIN;
ALTER EXTENSION timescaledb UPDATE TO :'TO_VERSION';
SELECT check_init_privs();
ROLLBACK;
\connect
SELECT check_init_privs();

\connect
ALTER EXTENSION timescaledb UPDATE TO :'TO_VERSION';
SELECT check_init_privs();

REASSIGN OWNED BY initacl_user TO CURRENT_USER;
DROP OWNED BY initacl_user;
DROP ROLE initacl_user;

-- Cleanup of the user role must preserve the other roles' initial grants and
-- dependencies. Their current privileges are checked explicitly because PUBLIC
-- also has SELECT on these relations.
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM saved_init_privs s
        JOIN pg_class c ON c.oid = s.relname::regclass
        WHERE NOT EXISTS (
            SELECT FROM aclexplode(c.relacl) a
            WHERE a.grantee = 'initacl_keep'::regrole AND a.privilege_type = 'SELECT')
    ) THEN
        RAISE EXCEPTION 'role cleanup changed another role''s privileges';
    END IF;
END
$$;
-- Derive the expected ACL from the saved value, independently of pg_class.
UPDATE saved_init_privs s SET relacl = ARRAY(
    SELECT item FROM unnest(s.relacl) WITH ORDINALITY a(item, ord)
    WHERE NOT EXISTS (
        SELECT FROM aclexplode(ARRAY[item]) x, saved_init_roles r
        WHERE r.rolname = 'initacl_user' AND r.oid IN (x.grantee, x.grantor))
    ORDER BY ord);
SELECT check_init_privs();
