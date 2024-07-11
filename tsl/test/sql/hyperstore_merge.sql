-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER;

\ir include/setup_hyperstore.sql

-- Disable merge and hash join to avoid flaky test.
set enable_mergejoin to false;
set enable_hashjoin to false;

-- There are already tests to merge into uncompressed tables, so just
-- compress all chunks using Hyperstore.
select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hyperstore');

create table source_data (
       created_at timestamptz not null,
       location_id smallint,
       device_id bigint,
       temp float8,
       humidity float4
);

INSERT INTO source_data(created_at, location_id, device_id, temp, humidity)
VALUES ('2022-06-01 00:00:00', 1, 1, 1.0, 1.0),
       ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0),
       ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0);

-- TODO(timescale/timescaledb-private#1170) Compression does not
-- support UPDATE and DELETE actions for MERGE command.
\set ON_ERROR_STOP 0
merge into :hypertable ht
using source_data sd on ht.created_at = sd.created_at
when matched then update set humidity = ht.humidity + sd.humidity;

merge into :hypertable ht
using source_data sd on ht.created_at = sd.created_at
when matched then delete;
\set ON_ERROR_STOP 1

-- Initially, there should be no uncompressed rows
\x on
select * from :hypertable where not _timescaledb_debug.is_compressed_tid(ctid);
\x off

-- Test merging into hypertable. The insert is executed since we use
-- ANALYZE, so we should see the number of arrays decompressed.
select explain_analyze_anonymize(format($$
    merge into %s ht
    using source_data sd on ht.created_at = sd.created_at
    when not matched then
	 insert (created_at, location_id, device_id, temp, humidity)
	 values (created_at, location_id, device_id, temp, humidity)
$$, :'hypertable'));

-- Now, the inserted rows should show up, but not the ones that
-- already exist.
\x on
select * from :hypertable where not _timescaledb_debug.is_compressed_tid(ctid);
\x off

-- Recompress all and try to insert the same rows again. This there
-- should be no rows inserted.
select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hyperstore');

\x on
select * from :hypertable where not _timescaledb_debug.is_compressed_tid(ctid);
\x off

-- Test merging into hypertable. The insert is executed since we use
-- ANALYZE, so we should see the number of arrays decompressed.
select explain_analyze_anonymize(format($$
    merge into %s ht
    using source_data sd on ht.created_at = sd.created_at
    when not matched then
	 insert (created_at, location_id, device_id, temp, humidity)
	 values (created_at, location_id, device_id, temp, humidity)
$$, :'hypertable'));

\x on
select * from :hypertable where not _timescaledb_debug.is_compressed_tid(ctid);
\x off
