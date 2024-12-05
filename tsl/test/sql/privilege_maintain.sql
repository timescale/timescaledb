-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE ROLE r_maintain;

CREATE TABLE metrics(time timestamptz, device text, value float);

SELECT create_hypertable('metrics', 'time');

INSERT INTO metrics SELECT generate_series('2020-01-01'::timestamptz, '2020-01-09'::timestamptz, '3 day'::interval);

\dp metrics
\dp _timescaledb_internal._hyper_*

GRANT MAINTAIN ON TABLE metrics TO r_maintain;

-- check privilege is present on all the chunks
\dp metrics
\dp _timescaledb_internal._hyper_*

REVOKE MAINTAIN ON TABLE metrics FROM r_maintain;

-- check privilege got removed from all the chunks
\dp metrics
\dp _timescaledb_internal._hyper_*

ALTER TABLE metrics SET (timescaledb.compress);
SELECT compress_chunk(chunk) FROM show_chunks('metrics') chunk;
GRANT MAINTAIN ON TABLE metrics TO r_maintain;

-- check privilege is present on all the chunks
\dp metrics
\dp _timescaledb_internal._hyper_*
\dp _timescaledb_internal.*compress*

REVOKE MAINTAIN ON TABLE metrics FROM r_maintain;

-- check privilege got removed from all the chunks
\dp metrics
\dp _timescaledb_internal._hyper_*
\dp _timescaledb_internal.*compress*

