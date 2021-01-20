-- For continuous aggregates: Copy ACL privileges (grants) from the
-- query view (user-facing object) to the internal objects (e.g.,
-- materialized hypertable, direct, and partial views). We want to
-- maintain the abstraction that a continuous aggregates is similar to
-- a materialized view (which is one object), so privileges on the
-- user-facing object should apply also to the internal objects that
-- implement the continuous aggregate. Having the right permissions on
-- internal objects is necessary for the watermark function used by
-- real-time aggregation since it queries the materialized hypertable
-- directly.

WITH rels_and_acl AS (
    -- For each cagg, collect an array of all relations (including
    -- chunks) to copy the ACL to
    SELECT array_cat(ARRAY[format('%I.%I', h.schema_name, h.table_name)::regclass,
	                       format('%I.%I', direct_view_schema, direct_view_name)::regclass,
                           format('%I.%I', partial_view_schema, partial_view_name)::regclass],
                     (SELECT array_agg(inhrelid::regclass)
                      FROM pg_inherits
                      WHERE inhparent = format('%I.%I', h.schema_name, h.table_name)::regclass)) AS relarr,
           relacl AS user_view_acl
    FROM _timescaledb_catalog.continuous_agg ca
    LEFT JOIN pg_class cl
    ON (cl.oid = format('%I.%I', user_view_schema, user_view_name)::regclass)
    LEFT JOIN _timescaledb_catalog.hypertable h
    ON (ca.mat_hypertable_id = h.id)
    WHERE relacl IS NOT NULL
)
-- Set the ACL on all internal cagg relations, including
-- chunks. Note that we cannot use GRANT statements because
-- such statements are recorded as privileges on extension
-- objects when run in an update script. The result is that
-- the privileges will become init privileges, which will then
-- be ignored by, e.g., pg_dump.
UPDATE pg_class
SET relacl = user_view_acl
FROM rels_and_acl
WHERE oid = ANY (relarr);
