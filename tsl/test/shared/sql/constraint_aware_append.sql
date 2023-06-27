-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test handling of ConstraintAwareAppend -> Sort -> Result -> Scan
-- issue 5739
CREATE TABLE ca_append_result(time timestamptz NULL, device text, value float);
SELECT table_name FROM create_hypertable('ca_append_result', 'time');
ALTER TABLE ca_append_result SET (timescaledb.compress);

INSERT INTO ca_append_result SELECT '2000-01-03','d1',0.3;
SELECT count(compress_chunk(ch)) AS compressed FROM show_chunks('ca_append_result') ch;
INSERT INTO ca_append_result SELECT '2000-01-13','d1',0.6;

SET enable_seqscan TO FALSE;
SELECT time_bucket('20d',time) AS day from ca_append_result WHERE time > '1900-01-01'::text::timestamp GROUP BY day;
RESET enable_seqscan;

DROP TABLE ca_append_result;

