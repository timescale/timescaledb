-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT
	extversion < '2.0.0' AS has_refresh_mat_view
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_before;
\else
CALL refresh_continuous_aggregate('mat_before',NULL,NULL);
\endif

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

-- Output ACLs for chunks on materialized hypertables. Chunk relation names are
-- renumbered across the upgrade, so normalize them.
SELECT inhparent::regclass::text AS parent,
       pg_temp.normalize_chunk(cl.oid::regclass::text) AS chunk,
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

-- Dump the invalidation log rows of the inval_log_test fixture so they
-- are part of the baseline/updated/restored comparison.
SELECT h.table_name AS hypertable,
       l.lowest_modified_value, l.greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log l
JOIN _timescaledb_catalog.hypertable h ON h.id = l.hypertable_id
WHERE h.table_name = 'inval_log_test'
ORDER BY 1, 2, 3;

SELECT ca.user_view_name AS cagg,
       l.lowest_modified_value, l.greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = l.materialization_id
WHERE ca.user_view_name IN ('mat_invallog_1', 'mat_invallog_2')
ORDER BY 1, 2, 3;

-- Verify the live invalidation logs still hold exactly the rows
-- snapshotted at the end of setup, i.e. an update script that rebuilds
-- the log catalogs neither lost nor invented rows.
DO $$
DECLARE
    difference TEXT;
BEGIN
    WITH live (log, name, lowest_modified_value, greatest_modified_value) AS (
        SELECT 'hypertable'::text, h.table_name,
               l.lowest_modified_value, l.greatest_modified_value
        FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log l
        JOIN _timescaledb_catalog.hypertable h ON h.id = l.hypertable_id
        WHERE h.table_name = 'inval_log_test'
        UNION ALL
        SELECT 'materialization', ca.user_view_name,
               l.lowest_modified_value, l.greatest_modified_value
        FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
        JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = l.materialization_id
        WHERE ca.user_view_name IN ('mat_invallog_1', 'mat_invallog_2')
    )
    SELECT string_agg(format('%s [%s]: (%s, %s, %s)', src, diff.log, diff.name,
                             diff.lowest_modified_value, diff.greatest_modified_value), E'\n')
    INTO difference
    FROM (
        SELECT 'missing after update' AS src, *
        FROM (SELECT * FROM inval_log_snapshot
              EXCEPT ALL
              SELECT * FROM live) missing
        UNION ALL
        SELECT 'unexpected after update', *
        FROM (SELECT * FROM live
              EXCEPT ALL
              SELECT * FROM inval_log_snapshot) unexpected
    ) diff (src, log, name, lowest_modified_value, greatest_modified_value);

    IF difference IS NOT NULL THEN
        RAISE EXCEPTION 'invalidation log content changed across the update'
              USING DETAIL = difference;
    END IF;

    IF NOT EXISTS (SELECT FROM inval_log_snapshot WHERE log = 'hypertable') OR
       NOT EXISTS (SELECT FROM inval_log_snapshot WHERE log = 'materialization') THEN
        RAISE EXCEPTION 'invalidation log snapshot is missing the expected pending rows';
    END IF;
END
$$ LANGUAGE PLPGSQL;
