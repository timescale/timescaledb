-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE EXTENSION timescaledb VERSION '2.30.2';
CREATE ROLE initacl_chain_root;
CREATE ROLE initacl_chain_middle;
CREATE ROLE initacl_chain_leaf;
CREATE TABLE chain_removed_role AS SELECT 'initacl_chain_root'::regrole::oid AS oid;

-- Record initial grant chains on real extension relations. The bgw_job chain
-- also has an independent grant-option path through the owner to middle.
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP VIEW _timescaledb_config.bgw_job;
GRANT INSERT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job
    TO initacl_chain_root WITH GRANT OPTION;
SET ROLE initacl_chain_root;
GRANT INSERT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job
    TO initacl_chain_middle WITH GRANT OPTION;
RESET ROLE;
GRANT INSERT ON _timescaledb_config.bgw_job TO initacl_chain_middle WITH GRANT OPTION;
SET ROLE initacl_chain_middle;
GRANT INSERT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job TO initacl_chain_leaf;
RESET ROLE;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb ADD VIEW _timescaledb_config.bgw_job;
REVOKE INSERT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job
    FROM initacl_chain_root CASCADE;

\connect
ALTER EXTENSION timescaledb UPDATE TO '2.31.0';
DROP OWNED BY initacl_chain_root;
DROP ROLE initacl_chain_root;

-- Give the surviving roles a valid current INSERT grant chain on both relations.
GRANT INSERT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job
    TO initacl_chain_middle WITH GRANT OPTION;
SET ROLE initacl_chain_middle;
GRANT INSERT ON _timescaledb_catalog.chunk, _timescaledb_config.bgw_job TO initacl_chain_leaf;
RESET ROLE;

CREATE TABLE chain_privs AS
SELECT c.oid::regclass::text AS relname, a.*
FROM pg_class c, aclexplode(c.relacl) a
WHERE c.oid IN ('_timescaledb_catalog.chunk'::regclass, '_timescaledb_config.bgw_job'::regclass);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_init_privs ip, aclexplode(ip.initprivs) a, chain_removed_role r
        WHERE ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
            AND ip.objoid = '_timescaledb_catalog.chunk'::regclass
            AND r.oid IN (a.grantee, a.grantor)
    ) THEN
        RAISE EXCEPTION 'missing dropped-role grant-chain fixture';
    END IF;
END
$$;

CREATE FUNCTION check_init_privs_grant_chain() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_init_privs ip, aclexplode(ip.initprivs) a, chain_removed_role r
        WHERE ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
            AND ip.objoid IN ('_timescaledb_catalog.chunk'::regclass, '_timescaledb_config.bgw_job'::regclass)
            AND r.oid IN (a.grantee, a.grantor)
    ) OR EXISTS (
        SELECT FROM pg_init_privs ip, aclexplode(ip.initprivs) a
        WHERE ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
            AND ip.objoid = '_timescaledb_catalog.chunk'::regclass
            AND a.grantee = 'initacl_chain_leaf'::regrole AND a.privilege_type = 'INSERT'
    ) THEN
        RAISE EXCEPTION 'abandoned initial grant chain remains';
    END IF;

    IF NOT EXISTS (
        SELECT FROM pg_init_privs ip, aclexplode(ip.initprivs) a
        WHERE ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
            AND ip.objoid = '_timescaledb_config.bgw_job'::regclass
            AND a.grantee = 'initacl_chain_middle'::regrole
            AND a.privilege_type = 'INSERT' AND a.is_grantable
    ) OR NOT EXISTS (
        SELECT FROM pg_init_privs ip, aclexplode(ip.initprivs) a
        WHERE ip.classoid = 'pg_class'::regclass AND ip.objsubid = 0
            AND ip.objoid = '_timescaledb_config.bgw_job'::regclass
            AND a.grantor = 'initacl_chain_middle'::regrole
            AND a.grantee = 'initacl_chain_leaf'::regrole AND a.privilege_type = 'INSERT'
    ) THEN
        RAISE EXCEPTION 'valid alternative initial grant chain was removed';
    END IF;

    IF EXISTS (
        (SELECT c.oid::regclass::text AS relname, a.* FROM pg_class c, aclexplode(c.relacl) a
         WHERE c.oid IN ('_timescaledb_catalog.chunk'::regclass, '_timescaledb_config.bgw_job'::regclass)
         EXCEPT SELECT * FROM chain_privs)
        UNION ALL
        (SELECT * FROM chain_privs EXCEPT
         SELECT c.oid::regclass::text AS relname, a.* FROM pg_class c, aclexplode(c.relacl) a
         WHERE c.oid IN ('_timescaledb_catalog.chunk'::regclass, '_timescaledb_config.bgw_job'::regclass))
    ) OR NOT has_table_privilege('initacl_chain_leaf', '_timescaledb_catalog.chunk', 'INSERT')
      OR NOT has_table_privilege('initacl_chain_leaf', '_timescaledb_config.bgw_job', 'INSERT') THEN
        RAISE EXCEPTION 'current grant chain changed during update or dump/restore';
    END IF;
END
$$;

\connect
BEGIN;
ALTER EXTENSION timescaledb UPDATE TO :'TO_VERSION';
SELECT check_init_privs_grant_chain();
ROLLBACK;
\connect
ALTER EXTENSION timescaledb UPDATE TO :'TO_VERSION';
SELECT check_init_privs_grant_chain();
