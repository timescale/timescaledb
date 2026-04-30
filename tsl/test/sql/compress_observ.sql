-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test the integration of the compression and observability features

CREATE TABLE t(ts timestamptz, a int, b int, c int, seg int);
SELECT create_hypertable('t', by_range('ts', interval '1 day'));

-- Initial config: bloom(a,b)
ALTER TABLE t SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a,b)'
);

INSERT INTO t
SELECT ts, (i % 10), (i % 5), (i % 3), 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

SELECT compress_chunk(c) FROM show_chunks('t') c;

-- omit timing information from the output
SELECT key_id,key_name,description,total_count FROM _timescaledb_functions.observ_keys();

-- remove the timing information so that the test is not flaky and replace event_id with a dense rank because it is also timing based
SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
      key_name, value
FROM _timescaledb_functions.observ_log(filter => '{"event_type": 1}')
WHERE key_name NOT IN ('agg_start', 'agg_end', 'relid', 'compress_relid');

SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
       payload - ARRAY['agg_start', 'agg_end', 'relid', 'compress_relid'] AS filtered_payload
FROM _timescaledb_functions.observ_log_events(filter => '{"event_type": 1}');

SELECT COUNT(*) FROM t;
SELECT sum(value) FROM _timescaledb_functions.observ_log(filter => '{"event_type": 1}') where key_name = 'batch_rows_sum';
SELECT value FROM _timescaledb_functions.observ_key_values('batch_rows_sum');
SELECT value FROM _timescaledb_functions.observ_log(filter => '{"event_type": 1}') where key_name = 'batch_rows_sum';

SELECT count(*),min(ts),max(ts) from t where a % 19 = 9 or b % 19 = 9 or c % 19 = 2 or seg % 19 = 2;

-- remove the timing information so that the test is not flaky and replace event_id with a dense rank because it is also timing based
SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
      key_name, value
FROM _timescaledb_functions.observ_log(filter => '{"event_type": 2, "cmd_type": 1}')
WHERE key_name NOT IN ('agg_start', 'agg_end', 'relid', 'compress_relid');

SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
       payload - ARRAY['agg_start', 'agg_end', 'relid', 'compress_relid'] AS filtered_payload
FROM _timescaledb_functions.observ_log_events(filter => '{"event_type": 2, "cmd_type": 1}');


-- Test DELETE stats
BEGIN;
DELETE FROM t WHERE a % 19 = 2;
ROLLBACK;

SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
       payload - ARRAY['agg_start', 'agg_end', 'relid', 'compress_relid'] AS filtered_payload
FROM _timescaledb_functions.observ_log_events(filter => '{"event_type": 2, "cmd_type": 4}');

SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
       payload - ARRAY['agg_start', 'agg_end', 'relid', 'compress_relid'] AS filtered_payload
FROM _timescaledb_functions.observ_log_events(filter => '{"event_type": 3, "cmd_type": 4}');

-- Test UPDATE stats
BEGIN;
UPDATE t SET a = 42 WHERE a % 19 = 2;
ROLLBACK;

SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
       payload - ARRAY['agg_start', 'agg_end', 'relid', 'compress_relid'] AS filtered_payload
FROM _timescaledb_functions.observ_log_events(filter => '{"event_type": 2, "cmd_type": 2}');
    
SELECT dense_rank() OVER (ORDER BY event_id) AS event_num,
       payload - ARRAY['agg_start', 'agg_end', 'relid', 'compress_relid'] AS filtered_payload
FROM _timescaledb_functions.observ_log_events(filter => '{"event_type": 3, "cmd_type": 2}');
