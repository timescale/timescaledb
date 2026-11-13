-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- A chunk can be owned by a different role than its hypertable, for example
-- when a superuser attaches a table owned by someone else. Owning the
-- hypertable must not be enough to drop or rewrite such a chunk.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE FUNCTION chunk_owners(hypertable REGCLASS)
RETURNS TABLE (chunk NAME, chunk_owner NAME) AS $$
	SELECT cl.relname, pg_get_userbyid(cl.relowner)
	FROM _timescaledb_catalog.chunk c
		JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
		JOIN pg_class cl ON cl.oid = c.relid
	WHERE format('%I.%I', h.schema_name, h.table_name)::regclass = hypertable
	ORDER BY 1
$$ LANGUAGE SQL;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE TABLE drop_permissions_test (time TIMESTAMPTZ NOT NULL, value INT);
SELECT create_hypertable('drop_permissions_test', 'time', chunk_time_interval => INTERVAL '1 day');
INSERT INTO drop_permissions_test VALUES ('2024-01-03 12:00+00', 3);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
CREATE TABLE attached_chunk_1 (time TIMESTAMPTZ NOT NULL, value INT);
CREATE TABLE attached_chunk_2 (time TIMESTAMPTZ NOT NULL, value INT);
INSERT INTO attached_chunk_1 VALUES ('2024-01-01 12:00+00', 1);
INSERT INTO attached_chunk_2 VALUES ('2024-01-02 12:00+00', 2);

-- The ranges have an explicit UTC offset so that they match the chunk
-- boundaries in any session timezone
\c :TEST_DBNAME :ROLE_SUPERUSER
CALL attach_chunk('drop_permissions_test', 'attached_chunk_1',
				  '{"time": ["2024-01-01T00:00:00+00", "2024-01-02T00:00:00+00"]}');
CALL attach_chunk('drop_permissions_test', 'attached_chunk_2',
				  '{"time": ["2024-01-02T00:00:00+00", "2024-01-03T00:00:00+00"]}');
SELECT * FROM chunk_owners('drop_permissions_test');

-- drop_chunks skips the chunks owned by another role
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT drop_chunks('drop_permissions_test', older_than => '2024-01-05T00:00:00+00'::timestamptz);
SELECT * FROM chunk_owners('drop_permissions_test');

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SELECT value FROM attached_chunk_1;
SELECT value FROM attached_chunk_2;

-- Dropping a single chunk needs ownership of both the chunk and the hypertable
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.drop_chunk('attached_chunk_1');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.drop_chunk('attached_chunk_2');
\set ON_ERROR_STOP 1

-- The OSM chunk is a foreign table, which can be owned by another role as well
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE EXTENSION postgres_fdw;
CREATE SERVER chunk_permissions_server FOREIGN DATA WRAPPER postgres_fdw
	OPTIONS (dbname :'TEST_DBNAME');
GRANT USAGE ON FOREIGN SERVER chunk_permissions_server TO :ROLE_DEFAULT_PERM_USER_2;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
CREATE FOREIGN TABLE foreign_osm_chunk (time TIMESTAMPTZ NOT NULL, value INT)
	SERVER chunk_permissions_server
	OPTIONS (schema_name 'public', table_name 'drop_permissions_test');

-- The hypertable owner cannot attach a foreign table they do not own
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.attach_osm_table_chunk('drop_permissions_test', 'foreign_osm_chunk');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_functions.attach_osm_table_chunk('drop_permissions_test', 'foreign_osm_chunk');
SELECT * FROM chunk_owners('drop_permissions_test');

-- Nor drop the OSM chunk
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.drop_osm_chunk('drop_permissions_test');
\set ON_ERROR_STOP 1

-- A superuser can drop everything
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT drop_chunks('drop_permissions_test', older_than => '2024-01-05T00:00:00+00'::timestamptz);
SELECT _timescaledb_functions.drop_osm_chunk('drop_permissions_test');
SELECT * FROM chunk_owners('drop_permissions_test');

-- reorder_chunk and move_chunk need ownership of the chunk as well
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE TABLE reorder_permissions_test (time TIMESTAMPTZ NOT NULL, value INT);
SELECT create_hypertable('reorder_permissions_test', 'time', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX reorder_permissions_test_value_idx ON reorder_permissions_test (value);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
CREATE TABLE attached_chunk_4 (time TIMESTAMPTZ NOT NULL, value INT);
INSERT INTO attached_chunk_4 VALUES ('2024-01-01 12:00+00', 1);

\c :TEST_DBNAME :ROLE_SUPERUSER
CALL attach_chunk('reorder_permissions_test', 'attached_chunk_4',
				  '{"time": ["2024-01-01T00:00:00+00", "2024-01-02T00:00:00+00"]}');

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT reorder_chunk('attached_chunk_4', 'reorder_permissions_test_value_idx');
SELECT move_chunk('attached_chunk_4', 'pg_default', 'pg_default');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
SELECT reorder_chunk('attached_chunk_4', 'reorder_permissions_test_value_idx');
SELECT move_chunk('attached_chunk_4', 'pg_default', 'pg_default');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT reorder_chunk('attached_chunk_4', 'reorder_permissions_test_value_idx');
SELECT move_chunk('attached_chunk_4', 'pg_default', 'pg_default');

-- The reorder policy skips the chunk and moves on to the next one. The three
-- most recent chunks are never reordered, so the hypertable needs more chunks.
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
INSERT INTO reorder_permissions_test
SELECT t, 1 FROM generate_series('2024-01-05 12:00+00'::timestamptz, '2024-01-09 12:00+00', INTERVAL '1 day') t;
SELECT add_reorder_policy('reorder_permissions_test', 'reorder_permissions_test_value_idx') AS job_id \gset
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :job_id \gset

-- Running the policy by hand still needs ownership of the hypertable
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
CALL _timescaledb_functions.policy_reorder(:job_id, :'config');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CALL run_job(:job_id);
CALL run_job(:job_id);
SELECT c.relid::regclass AS chunk, s.num_times_job_run
FROM _timescaledb_internal.bgw_policy_chunk_stats s
	JOIN _timescaledb_catalog.chunk c ON c.id = s.chunk_id
WHERE s.job_id = :job_id
ORDER BY 1;
