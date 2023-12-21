-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test unique expression indexes

CREATE TABLE compress_unique(offset_timestamp timestamptz not null, meter_id text, meter_channel_id text, timestamp timestamptz);
SELECT table_name FROM create_hypertable('compress_unique','offset_timestamp');

CREATE UNIQUE INDEX uniq_expr ON compress_unique USING btree (lower((meter_id)::text), meter_channel_id, offset_timestamp, "timestamp");
ALTER TABLE compress_unique SET (timescaledb.compress,timescaledb.compress_segmentby='meter_id,meter_channel_id');

INSERT INTO compress_unique VALUES ('2000-01-01','m1','c1','2000-01-01');
INSERT INTO compress_unique VALUES ('2000-01-01','m1','c2','2000-01-01');

SELECT compress_chunk(show_chunks('compress_unique')) IS NOT NULL AS compress;

-- should fail
\set ON_ERROR_STOP 0
INSERT INTO compress_unique VALUES ('2000-01-01','m1','c2','2000-01-01');
\set ON_ERROR_STOP 1

SELECT * FROM compress_unique ORDER BY compress_unique;

DROP TABLE compress_unique;

