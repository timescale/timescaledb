-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\x on
SELECT * FROM mat_before ORDER BY bucket, location;
\x off

--cause invalidations in the time range that is already
--materialized. However, shift time by one second so that each
--(timestamp, location) pair is unique. Otherwise last(temperature,
--timec) won't be deterministic.
INSERT INTO conditions_before
SELECT generate_series('2018-12-01 00:01'::timestamp, '2018-12-31 00:01'::timestamp, '1 day'), 'POR', 165, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;

--cause invalidations way in the past
INSERT INTO conditions_before
SELECT generate_series('2017-12-01 00:01'::timestamp, '2017-12-31 00:01'::timestamp, '1 day'), 'POR', 1065, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;

\x on
SELECT * FROM mat_before ORDER BY bucket, location;
\x off

CALL refresh_continuous_aggregate('mat_before',NULL,NULL);

--the max of the temp for the POR should now be 165
\x on
SELECT * FROM mat_before ORDER BY bucket, location;
\x off

-- Output the ACLs for each internal cagg object
SELECT cl.oid::regclass::text AS reloid,
       unnest(relacl)::text AS relacl
FROM _timescaledb_catalog.continuous_agg ca
JOIN _timescaledb_catalog.hypertable h
ON (ca.mat_hypertable_id = h.id)
JOIN pg_class cl
ON (cl.oid IN (format('%I.%I', h.schema_name, h.table_name)::regclass,
               format('%I.%I', direct_view_schema, direct_view_name)::regclass,
               format('%I.%I', partial_view_schema, partial_view_name)::regclass))
ORDER BY reloid, relacl;

-- Output ACLs for chunks on materialized hypertables
SELECT inhparent::regclass::text AS parent,
       cl.oid::regclass::text AS chunk,
       unnest(relacl)::text AS acl
FROM _timescaledb_catalog.continuous_agg ca
JOIN _timescaledb_catalog.hypertable h
ON (ca.mat_hypertable_id = h.id)
JOIN pg_inherits inh ON (inh.inhparent = format('%I.%I', h.schema_name, h.table_name)::regclass)
JOIN pg_class cl
ON (cl.oid = inh.inhrelid)
ORDER BY parent, chunk, acl;

-- Verify privileges on internal cagg objects.  The privileges on the
-- materialized hypertable, partial view, and direct view should match
-- the user-facing user view.
DO $$
DECLARE
    user_view_rel regclass;
    user_view_acl aclitem[];
    rel regclass;
    acl aclitem[];
    acl_matches boolean;
BEGIN
    FOR user_view_rel, user_view_acl IN
        SELECT cl.oid, cl.relacl
        FROM pg_class cl
        JOIN _timescaledb_catalog.continuous_agg ca
        ON (format('%I.%I', ca.user_view_schema, ca.user_view_name)::regclass = cl.oid)
    LOOP
        FOR rel, acl, acl_matches IN
            SELECT cl.oid,
                   cl.relacl,
                   COALESCE(cl.relacl, ARRAY[]::aclitem[]) @> COALESCE(user_view_acl, ARRAY[]::aclitem[])
            FROM _timescaledb_catalog.continuous_agg ca
            JOIN _timescaledb_catalog.hypertable h
            ON (ca.mat_hypertable_id = h.id)
            JOIN pg_class cl
            ON (cl.oid IN (format('%I.%I', h.schema_name, h.table_name)::regclass,
                           format('%I.%I', direct_view_schema, direct_view_name)::regclass,
                           format('%I.%I', partial_view_schema, partial_view_name)::regclass))
            WHERE format('%I.%I', ca.user_view_schema, ca.user_view_name)::regclass = user_view_rel
        LOOP
            IF NOT acl_matches THEN
               RAISE EXCEPTION 'privileges mismatch for continuous aggregate "%"', user_view_rel
                     USING DETAIL = format('Privileges for internal object "%s" are [%s], expected [%s].',
                            rel, acl, user_view_acl);
            END IF;
        END LOOP;
    END LOOP;
END
$$ LANGUAGE PLPGSQL;
