# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Granular-refresh behavior across a postmaster restart / crash.  The
# per-hypertable tenant tracker lives in shared memory, so a restart wipes it.
#
#   Test 1 (seqnum reseed): on the first touch after a restart the tracker
#   must be re-seeded to (max durable seqnum + 1), taken over every durable
#   source that can still carry a pre-restart seqnum -- the hypertable
#   invalidation log, each cagg's materialization invalidation log, and the
#   tenant-tracking catalog -- so post-restart seqnums never collide with
#   pre-restart ones.
#
#   Test 2 (crash fallback): In-memory trackings are lost after a crash, but their
#   associated log entries survive.  After a restart there are no tenant_tracking
#   rows for that seqnum, so the refresh must fall back to a full refresh of the
#   invalidated range and still produce the correct aggregate.
#

use strict;
use warnings;
use TimescaleNode;
use Test::More;

my $node = TimescaleNode->create('granular_restart');

# Deterministic timezone for the fixed timestamps below.
$node->safe_psql('postgres', q{ALTER SYSTEM SET timezone = 'UTC';});
$node->restart;

# Bind the debug-only tracker inspector.  The regression include creates it via
# the :TSL_MODULE_PATHNAME psql variable, which does not exist under TAP, so
# derive the tsl module name from the installed extension version.
my $ver = $node->safe_psql('postgres',
	q{SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'});

# ===========================================================================
# Test 1: seqnum is reseeded to (max durable seqnum + 1) after a restart.
# ===========================================================================

# Hypertable configured for granular tracking on the "sensor_id" tenant column
$node->safe_psql(
	'postgres', q{
    CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('conditions', 'time');
    ALTER TABLE conditions SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '100 years',
        timescaledb.granular_refresh_end_offset = '1 day');
});
$node->safe_psql(
	'postgres', q{
    CREATE MATERIALIZED VIEW cond_daily
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
        FROM conditions GROUP BY bucket, sensor_id
        WITH NO DATA;
});
$node->safe_psql('postgres',
	q{ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true)}
);

my $htid = $node->safe_psql('postgres',
	q{SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'conditions'}
);

# Run refresh to establish the materialization threshold first, so later inserts into the
# already-materialized region register as seqnum-stamped invalidations.

$node->safe_psql('postgres',
	q{CALL refresh_continuous_aggregate('cond_daily', NULL, '2025-05-01 00:00+00')}
);

# Two insert+refresh cycles: each refresh flushes the tracker, advancing the
# seqnum and persisting tenant-tracking rows.

# This first insert initializes the tracker and sets its seqnum to 1.
$node->safe_psql('postgres',
	q{INSERT INTO conditions VALUES ('2020-01-01 00:00+00','sensor_a',1),('2020-01-01 06:00+00','sensor_b',2)}
);

# Seqnum 1 is flushed to the catalog, and the tracker is advanced to seqnum 2.
$node->safe_psql('postgres',
	q{CALL refresh_continuous_aggregate('cond_daily', NULL, '2025-05-01 00:00+00')}
);
$node->safe_psql('postgres',
	q{INSERT INTO conditions VALUES ('2020-01-02 00:00+00','sensor_a',3),('2020-01-02 06:00+00','sensor_c',4)}
);

# Seqnum 2 is flushed to the catalog, and the tracker is advanced to seqnum 3.
$node->safe_psql('postgres',
	q{CALL refresh_continuous_aggregate('cond_daily', NULL, '2025-05-01 00:00+00')}
);

# This insert writes invalidation log entry with seqnum = 3.
# Note that tracker entries for seqnum=3 will not get flushed as we have an instance failure
# before any other refreshes.
$node->safe_psql('postgres',
	q{INSERT INTO conditions VALUES ('2020-01-03 00:00+00','sensor_a',5)});

my $pre_seq = $node->safe_psql('postgres',
	q{SELECT seq_num FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('conditions')}
);
note("pre-restart tracker seqnum: '$pre_seq'");
is($pre_seq, 3, 'tracker seqnum is 3 before restart');

# ---- restart wipes the shared-memory tracker ----
$node->restart;

my $cleared = $node->safe_psql('postgres',
	q{SELECT seq_num FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('conditions')}
);
is($cleared, '', 'restart cleared the in-memory tracker');

# Max durable seqnum across all three sources, computed after the
# restart but before any new insert.
my $max_durable = $node->safe_psql(
	'postgres', qq{
    SELECT coalesce(max(s), 0) FROM (
        SELECT max(seqnum) s
          FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
         WHERE hypertable_id = $htid
        UNION ALL
        SELECT max(seqnum)
          FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
         WHERE materialization_id IN (
             SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
              WHERE raw_hypertable_id = $htid)
        UNION ALL
        SELECT max(seqnum)
          FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
         WHERE hypertable_id = $htid
    ) q});
note("max durable seqnum: '$max_durable'");
is($max_durable, 3, 'max durable seqnum across the three sources is 3');

# First touch after restart re-creates the tracker; its seqnum must be seeded with max_durable + 1.
$node->safe_psql('postgres',
	q{INSERT INTO conditions VALUES ('2020-01-04 00:00+00','sensor_a',6)});
my $post_seq = $node->safe_psql('postgres',
	q{SELECT seq_num FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('conditions')}
);
note("post-restart reseeded seqnum: '$post_seq'");
is( $post_seq,
	$max_durable + 1,
	'seqnum reseeded to (max durable seqnum + 1) after restart');

# ===========================================================================
# Test 1b: Base case - seqnum is seeded to 1 if there are no prior durable seqnums.
# ===========================================================================

$node->safe_psql(
	'postgres', q{
    CREATE TABLE fresh(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('fresh', 'time');
    ALTER TABLE fresh SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '100 years',
        timescaledb.granular_refresh_end_offset = '1 day');
});
$node->safe_psql(
	'postgres', q{
    CREATE MATERIALIZED VIEW fresh_daily
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
        FROM fresh GROUP BY bucket, sensor_id
        WITH NO DATA;
});
$node->safe_psql('postgres',
	q{ALTER MATERIALIZED VIEW fresh_daily SET (timescaledb.enable_granular_refresh = true)}
);
$node->safe_psql('postgres',
	q{INSERT INTO fresh VALUES ('2020-02-01 00:00+00','sensor_a',1)});
my $fresh_seq = $node->safe_psql('postgres',
	q{SELECT seq_num FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('fresh')}
);
is($fresh_seq, 1, 'tracker with no durable history seeds to 1');

# ===========================================================================
# Test 2: a crash loses unflushed in-memory trackings; the refresh must
# fall back to a full refresh for the orphaned seqnum and stay correct.
# ===========================================================================

$node->safe_psql(
	'postgres', q{
    CREATE TABLE crash_ht(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('crash_ht', 'time');
    ALTER TABLE crash_ht SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '100 years',
        timescaledb.granular_refresh_end_offset = '1 day');
});
$node->safe_psql(
	'postgres', q{
    CREATE MATERIALIZED VIEW crash_daily
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
        FROM crash_ht GROUP BY bucket, sensor_id
        WITH NO DATA;
});
$node->safe_psql('postgres',
	q{ALTER MATERIALIZED VIEW crash_daily SET (timescaledb.enable_granular_refresh = true)}
);

my $crash_htid = $node->safe_psql('postgres',
	q{SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'crash_ht'}
);

# Establish the threshold, then insert two tenants' late-arriving data.  These
# are tracked in shared memory and logged with a seqnum, but NOT flushed (no
# refresh follows before the crash).
$node->safe_psql('postgres',
	q{CALL refresh_continuous_aggregate('crash_daily', NULL, '2025-05-01 00:00+00')}
);
$node->safe_psql(
	'postgres', q{
    INSERT INTO crash_ht VALUES
        ('2020-01-01 00:00+00','sensor_a',1),
        ('2020-01-02 00:00+00','sensor_a',2),
        ('2020-01-01 00:00+00','sensor_b',3);
});

my $logged_seqnum = $node->safe_psql(
	'postgres', qq{
    SELECT coalesce(max(seqnum), 0)
      FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
     WHERE hypertable_id = $crash_htid});
is($logged_seqnum, 1,
	'pre-crash inserts stamped seqnum 1 in the invalidation log');

my $tracked_before = $node->safe_psql(
	'postgres', qq{
    SELECT count(*) FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
     WHERE hypertable_id = $crash_htid});
is($tracked_before, 0, 'no tenant-tracking rows flushed before the crash');

# ---- crash: immediate stop drops shared memory without flushing ----
$node->stop('immediate');
$node->start;

my $tracked_after = $node->safe_psql(
	'postgres', qq{
    SELECT count(*) FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
     WHERE hypertable_id = $crash_htid});
is($tracked_after, 0, 'crash lost the in-memory trackings (none persisted)');

my $logged_after = $node->safe_psql(
	'postgres', qq{
    SELECT coalesce(max(seqnum), 0)
      FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
     WHERE hypertable_id = $crash_htid});
is($logged_after, 1, 'seqnum-1 invalidations survived the crash');

# The orphaned seqnum 1 (its trackings lost to the crash) is still the max
# durable seqnum, so a first-touch insert after the crash must reseed the tracker
# ABOVE it -- to 2, never reusing 1 -- so recovered data and the orphaned entries
# can never share a seqnum.
$node->safe_psql('postgres',
	q{INSERT INTO crash_ht VALUES ('2020-01-03 00:00+00','sensor_a',4)});
my $reseed = $node->safe_psql('postgres',
	q{SELECT seq_num FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('crash_ht')}
);
note("post-crash reseeded seqnum: '$reseed'");
is($reseed, 2,
	'post-crash first touch reseeds the seqnum to 2 (above orphaned seqnum 1)'
);

# Refresh: for the orphaned seqnum there are no tenant-tracking rows, so this
# must fall back to a full refresh of the invalidated range.
$node->safe_psql('postgres',
	q{CALL refresh_continuous_aggregate('crash_daily', NULL, '2025-05-01 00:00+00')}
);

# The cagg must exactly match a direct aggregation over the raw data -- i.e. the
# fallback materialized both tenants correctly.
my $symdiff = $node->safe_psql(
	'postgres', q{
    WITH expected AS (
        SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value) AS avg
        FROM crash_ht GROUP BY 1, 2),
    got AS (SELECT bucket, sensor_id, avg FROM crash_daily)
    SELECT (SELECT count(*) FROM (SELECT * FROM expected EXCEPT SELECT * FROM got) a)
         + (SELECT count(*) FROM (SELECT * FROM got EXCEPT SELECT * FROM expected) b)}
);
is($symdiff, 0, 'cagg matches full aggregation after crash-fallback refresh');

my $rows = $node->safe_psql('postgres',
	q{SELECT count(*) FROM crash_daily WHERE sensor_id IN ('sensor_a','sensor_b')}
);
is($rows, 4, 'fallback materialized all buckets (sensor_a x3, sensor_b x1)');

done_testing();
