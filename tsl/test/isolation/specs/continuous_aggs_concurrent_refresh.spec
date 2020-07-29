# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Setup prior to every permutation.
#
# We define a function 'cagg_bucket_count' to get the number of
# buckets in a continuous aggregate.  We use it to verify that there
# aren't any duplicate buckets inserted after concurrent
# refreshes. Duplicate buckets are possible since there is no unique
# constraint on the GROUP BY keys in the materialized hypertable.
#
setup
{
    SELECT _timescaledb_internal.stop_background_workers();
    CREATE TABLE conditions(time timestamptz, temp float);
    SELECT create_hypertable('conditions', 'time');
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(t::timestamp))%40
    FROM generate_series('2020-05-01', '2020-05-10', '10 minutes'::interval) t;
    CREATE VIEW daily_temp
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1 day', time) AS day, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1;
    CREATE VIEW weekly_temp
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1 week', time) AS day, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1;

    CREATE OR REPLACE FUNCTION cagg_bucket_count(cagg regclass) RETURNS int
    AS $$
    DECLARE
      cagg_schema name;
      cagg_name name;
      cagg_hyper_schema name;
      cagg_hyper_name name;
      cagg_hyper_relid regclass;
      result int;
    BEGIN
      SELECT nspname, relname
      INTO cagg_schema, cagg_name
      FROM pg_class c, pg_namespace n
      WHERE c.oid = cagg
      AND c.relnamespace = n.oid;

      SELECT format('%I.%I', h.schema_name, h.table_name)::regclass, h.schema_name, h.table_name
      INTO cagg_hyper_relid, cagg_hyper_schema, cagg_hyper_name
      FROM _timescaledb_catalog.continuous_agg ca, _timescaledb_catalog.hypertable h
      WHERE ca.user_view_name = cagg_name
      AND ca.user_view_schema = cagg_schema
      AND ca.mat_hypertable_id = h.id;

      EXECUTE format('SELECT count(*) FROM %I.%I',
                quote_ident(cagg_hyper_schema),
                quote_ident(cagg_hyper_name))
      INTO result;

      RETURN result;
    END
    $$ LANGUAGE plpgsql;
}

teardown {
    DROP TABLE conditions CASCADE;
}

# Session to refresh the daily_temp continuous aggregate
session "R1"
setup
{
    BEGIN;
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '500ms';
}
step "R1_refresh"
{
    SELECT refresh_continuous_aggregate('daily_temp', '2020-05-01', '2020-05-02');
}
step "R1_commit"
{
    COMMIT;
}

# Refresh that overlaps with R1
session "R2"
setup
{
    BEGIN;
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '500ms';
}
step "R2_refresh"
{
    SELECT refresh_continuous_aggregate('daily_temp', '2020-05-01', '2020-05-02');
}
step "R2_commit"
{
    COMMIT;
}

# Refresh on same aggregate (daily_temp) that doesn't overlap with R1 and R2
session "R3"
setup
{
    BEGIN;
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '500ms';
}
step "R3_refresh"
{
    SELECT refresh_continuous_aggregate('daily_temp', '2020-05-08', '2020-05-10');
}
step "R3_commit"
{
    COMMIT;
}

# Overlapping refresh on another continuous aggregate (weekly_temp)
session "R4"
setup
{
    BEGIN;
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '500ms';
}
step "R4_refresh"
{
    SELECT refresh_continuous_aggregate('weekly_temp', '2020-05-01', '2020-05-10');
}
step "R4_commit"
{
    COMMIT;
}

# Session to query
session "S1"
setup
{
    SET LOCAL lock_timeout = '500ms';
    SET LOCAL deadlock_timeout = '100ms';
}
step "S1_select"
{
    SELECT day, avg_temp
    FROM daily_temp
    ORDER BY 1;

    SELECT * FROM cagg_bucket_count('daily_temp');
}

# Run single transaction refresh to get some reference output.
# The result of a query on the aggregate should always look like this example.
permutation "R1_refresh" "R1_commit" "S1_select" "R2_commit" "R3_commit" "R4_commit"

# Interleave two refreshes that are overlapping. Since we serialize
# refreshes, R2 should block until R1 commits.
permutation "R1_refresh" "R2_refresh" "R1_commit" "R2_commit" "S1_select" "R3_commit" "R4_commit"

# R2 starts after R1 but commits before. This should not work (lock timeout).
permutation "R1_refresh" "R2_refresh" "R2_commit" "R1_commit" "S1_select" "R3_commit" "R4_commit"

# R1 and R3 don't have overlapping refresh windows, but we serialize
# anyway, so not yet supported.
permutation "R1_refresh" "R3_refresh" "R3_commit" "R1_commit" "S1_select" "R2_commit" "R4_commit"

# Concurrent refreshing across two different aggregates on the same
# hypertable should be OK:
permutation "R1_refresh" "R4_refresh" "R4_commit" "R1_commit" "S1_select" "R2_commit" "R3_commit"
