# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE ROLE cagg_role;

    CREATE TABLE src (ts timestamptz NOT NULL, val double precision, temp double precision);
    SELECT create_hypertable('src', 'ts', chunk_time_interval => INTERVAL '1 day');
    INSERT INTO src
    SELECT '2026-01-01'::timestamptz + i*INTERVAL '1 hour', i, i * 0.1
    FROM generate_series(0, 23) i;

    CREATE MATERIALIZED VIEW cagg_a
    WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
    SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
    FROM src GROUP BY bucket WITH NO DATA;

    CREATE MATERIALIZED VIEW cagg_parent
    WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
    SELECT time_bucket(INTERVAL '1 hour', ts) AS bucket, avg(val) AS avg_val
    FROM src GROUP BY bucket WITH NO DATA;

    CREATE MATERIALIZED VIEW cagg_child
    WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
    SELECT time_bucket(INTERVAL '1 day', bucket) AS day, avg(avg_val) AS daily
    FROM cagg_parent GROUP BY day WITH NO DATA;
}

teardown
{
    DROP MATERIALIZED VIEW IF EXISTS cagg_child;
    DROP MATERIALIZED VIEW IF EXISTS cagg_parent;
    DROP MATERIALIZED VIEW IF EXISTS cagg_a;
    DROP TABLE IF EXISTS src;
    DROP ROLE IF EXISTS cagg_role;
}

session "wp"
step "wp_before_uv_enable"  { SELECT debug_waitpoint_enable('cagg_add_column_before_uv_lock'); }
step "wp_before_uv_release" { SELECT debug_waitpoint_release('cagg_add_column_before_uv_lock'); }
step "wp_before_ht_enable"  { SELECT debug_waitpoint_enable('cagg_add_column_before_ht_lock'); }
step "wp_before_ht_release" { SELECT debug_waitpoint_release('cagg_add_column_before_ht_lock'); }
step "wp_before_pv_enable"    { SELECT debug_waitpoint_enable('cagg_add_column_before_pv_lock'); }
step "wp_before_pv_release"   { SELECT debug_waitpoint_release('cagg_add_column_before_pv_lock'); }
step "wp_after_enable"      { SELECT debug_waitpoint_enable('cagg_add_column_after_locks'); }
step "wp_after_release"     { SELECT debug_waitpoint_release('cagg_add_column_after_locks'); }
step "wp_refresh_enable"    { SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock'); }
step "wp_refresh_release"   { SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock'); }

session "s1"
step "s1_add_a"      { ALTER MATERIALIZED VIEW cagg_a      ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp))      STORED; }
step "s1_add_parent" { ALTER MATERIALIZED VIEW cagg_parent ADD COLUMN min_temp double precision GENERATED ALWAYS AS (min(temp))      STORED; }

session "s2"
step "s2_add_a"     { ALTER MATERIALIZED VIEW cagg_a     ADD COLUMN max_temp  double precision GENERATED ALWAYS AS (max(temp))      STORED; }
step "s2_add_child" { ALTER MATERIALIZED VIEW cagg_child ADD COLUMN max_daily double precision GENERATED ALWAYS AS (max(avg_val))   STORED; }

session "r"
step "r_refresh_a" { CALL refresh_continuous_aggregate('cagg_a', '2026-01-01', '2026-01-02'); }

session "reader"
step "reader_begin"  { BEGIN; SELECT count(*) FROM cagg_a; }
step "reader_commit" { COMMIT; }

session "dropper"
step "drop_a" { DROP MATERIALIZED VIEW cagg_a; }

# Other (non ADD COLUMN) ALTERs on the same cagg
session "alt"
step "alt_matonly"  { ALTER MATERIALIZED VIEW cagg_a SET (timescaledb.materialized_only = false); }
step "alt_chunk"    { ALTER MATERIALIZED VIEW cagg_a SET (timescaledb.chunk_interval = '2 days'); }
step "alt_owner"    { ALTER MATERIALIZED VIEW cagg_a OWNER TO cagg_role; }
step "alt_compress" { ALTER MATERIALIZED VIEW cagg_a SET (timescaledb.compress = true); }
step "alt_rename"   { ALTER MATERIALIZED VIEW cagg_a RENAME COLUMN avg_val TO mean_val; }

session "v"
step "v_cols_a"      { SELECT column_name FROM information_schema.columns WHERE table_name='cagg_a'      ORDER BY ordinal_position; }
step "v_cols_parent" { SELECT column_name FROM information_schema.columns WHERE table_name='cagg_parent' ORDER BY ordinal_position; }
step "v_cols_child"  { SELECT column_name FROM information_schema.columns WHERE table_name='cagg_child'  ORDER BY ordinal_position; }
step "v_exists_a"    { SELECT count(*) AS cagg_a_exists FROM pg_class WHERE relname='cagg_a' AND relkind='v'; }
step "v_matonly_a"   { SELECT materialized_only FROM timescaledb_information.continuous_aggregates WHERE view_name='cagg_a'; }
step "v_owner_a"     { SELECT pg_get_userbyid(relowner) AS owner FROM pg_class WHERE relname='cagg_a'; }
step "v_compress_a"  { SELECT compression_enabled FROM timescaledb_information.continuous_aggregates WHERE view_name='cagg_a'; }

# Two concurrent ADD COLUMNs serialize on the user-view lock alone.
# s1 pauses BEFORE mat HT lock (so it holds only the user view), then s2 blocks
permutation "wp_before_ht_enable" "s1_add_a" "wp_before_uv_enable" "s2_add_a" "wp_before_ht_release" "wp_before_uv_release" "v_cols_a"

# Refresh holds the lock first; ADD COLUMN waits, then runs.
permutation "wp_refresh_enable" "r_refresh_a" "s1_add_a"("r_refresh_a") "wp_refresh_release" "v_cols_a"

# ADD COLUMN holds all locks; refresh waits, both succeed in order.
permutation "wp_after_enable" "s1_add_a" "r_refresh_a"("s1_add_a") "wp_after_release" "v_cols_a"

# Hierarchical CAggs: ADD COLUMN on parent + child concurrently, both succeed.
# Child does not pick up the parent's new column (it isn't referenced in child's query).
permutation "wp_after_enable" "s1_add_parent" "s2_add_child"("s1_add_parent") "wp_after_release" "v_cols_parent" "v_cols_child"

# ADD COLUMN pauses BEFORE taking any lock; a reader can still acquire
# AccessShareLock on the user view. Once ADD resumes, it blocks behind the
# reader until commit.
permutation "wp_before_uv_enable" "s1_add_a" "reader_begin" "wp_before_uv_release" "reader_commit" "v_cols_a"

# ADD COLUMN holds the user-view lock; DROP waits, then drops the cagg.
permutation "wp_after_enable" "s1_add_a" "drop_a"("s1_add_a") "wp_after_release" "v_exists_a"

# DROP takes locks first; subsequent ADD COLUMN errors because the cagg is gone.
permutation "wp_before_uv_enable" "s1_add_a" "drop_a" "wp_before_uv_release"

# No deadlock cases due to non-conflicting locks
permutation "wp_before_pv_enable" "s1_add_a" "alt_matonly"("s1_add_a") "wp_before_pv_release" "v_cols_a" "v_matonly_a"
permutation "wp_before_pv_enable" "s1_add_a" "alt_chunk"("s1_add_a")   "wp_before_pv_release" "v_cols_a"
permutation "wp_before_pv_enable" "s1_add_a" "alt_owner"("s1_add_a")   "wp_before_pv_release" "v_cols_a" "v_owner_a"

# DEADLOCK REPRODUCER due to different ordering in locks taken
#
# e.g. ADD COLUMN locks the cagg relations as user view -> mat HT -> partial view
# -> direct view, whereas RENAME COLUMN locks them in the reverse order
# (direct view -> partial view -> mat HT -> user view).
#
# Here ADD COLUMN pauses BEFORE the mat-HT lock, holding only the user-view
# lock. RENAME COLUMN then takes the direct view, partial view and mat HT and
# finally blocks waiting for the user view (held by ADD COLUMN). When ADD COLUMN
# resumes it tries to take the mat HT (now held by RENAME COLUMN) and the two
# deadlock.
permutation "wp_before_pv_enable" "s1_add_a" "alt_rename"("s1_add_a")   "wp_before_pv_release" "v_cols_a"
permutation "wp_before_pv_enable" "s1_add_a" "alt_compress"("s1_add_a") "wp_before_pv_release" "v_cols_a" "v_compress_a"
