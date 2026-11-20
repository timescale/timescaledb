-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Use a fresh connection after the version change. Check counts, not just
-- membership, so duplicate dependencies cannot make this test pass.
DO $$
DECLARE
    expected acl_epq_expected;
    actual_roles oid[];
    expected_roles oid[];
BEGIN
    SELECT * INTO STRICT expected FROM acl_epq_expected;
    IF (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb')
        IS DISTINCT FROM expected.target_version THEN
        RAISE EXCEPTION 'concurrent extension update did not complete';
    END IF;
    IF (SELECT relacl FROM pg_class WHERE oid = expected.relid)
        IS DISTINCT FROM expected.relacl OR
       (SELECT initprivs FROM pg_init_privs WHERE objoid = expected.relid
        AND classoid = 'pg_class'::regclass AND objsubid = 0)
        IS DISTINCT FROM expected.initprivs THEN
        RAISE EXCEPTION 'concurrent extension update did not restore the saved ACLs';
    END IF;

    SELECT array_agg(refobjid ORDER BY refobjid) INTO actual_roles
    FROM pg_shdepend
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        AND classid = 'pg_class'::regclass AND objid = expected.relid AND objsubid = 0
        AND refclassid = 'pg_authid'::regclass
        AND deptype = CASE WHEN expected.initial THEN 'i'::"char" ELSE 'a'::"char" END;
    SELECT array_agg(oid ORDER BY oid) INTO expected_roles FROM pg_roles
    WHERE rolname = 'acl_epq_missing'
        OR (expected.initial AND rolname = 'acl_epq_concurrent');
    IF actual_roles IS DISTINCT FROM expected_roles THEN
        RAISE EXCEPTION 'unexpected or duplicate ACL dependencies: expected %, got %',
            expected_roles, actual_roles;
    END IF;
END
$$;
