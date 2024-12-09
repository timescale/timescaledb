-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO PST8PDT;

\ir include/cagg_refresh_common.sql

-- Test forceful refreshment. Here we simulate the situation that we've seen
-- with tiered data when `timescaledb.enable_tiered_reads` were disabled on the
-- server level. In that case we would not see materialized tiered data and
-- we wouldn't be able to re-materialize the data using a normal refresh call
-- because it would skip previously materialized ranges, but it should be
-- possible with `force=>true` parameter. To simulate this use-case we clear
-- the materialization hypertable and forefully re-materialize it.
SELECT ht.schema_name || '.' || ht.table_name AS mat_ht, mat_hypertable_id FROM _timescaledb_catalog.continuous_agg cagg
JOIN _timescaledb_catalog.hypertable ht ON cagg.mat_hypertable_id = ht.id
WHERE user_view_name = 'daily_temp' \gset

-- Delete the data from the materialization hypertable
DELETE FROM :mat_ht;

SET timezone TO UTC;
-- Run regular refresh, it should not touch previously materialized range
CALL refresh_continuous_aggregate('daily_temp', '2020-05-04 00:00 UTC', '2020-05-05 00:00 UTC');
SELECT * FROM daily_temp
ORDER BY day, device;
-- Run it again with force=>true, the data should be rematerialized
CALL refresh_continuous_aggregate('daily_temp', '2020-05-04 00:00 UTC', '2020-05-05 00:00 UTC', force=>true);
SELECT * FROM daily_temp
ORDER BY day, device;

