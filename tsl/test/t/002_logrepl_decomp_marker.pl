# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Test::More;

# This test checks the creation of logical replication messages
# used to mark the start and end of activity happening as a result
# compression/decompression.

# Publishing node
my $db = TimescaleNode->create('publisher', allows_streaming => 'logical');

sub run_queries
{
	foreach my $query (@_)
	{
		$db->safe_psql('postgres', $query);
	}
}

sub query_generates_wal
{
	my $description  = shift @_;
	my $query        = shift @_;
	my $expected_wal = shift @_;

	$db->safe_psql('postgres', $query);
	my $result = $db->safe_psql('postgres',
		qq/SELECT data FROM pg_logical_slot_get_changes('slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');/
	);

	# Remove BEGIN/COMMIT from output since there are only single
	# transactions in the output and the only interesting part from a
	# testing perspective are the other messages.
	$result =~ s/^(BEGIN|COMMIT).*\n?//gm;

	# creation_time is non-deterministic, so strip it from wal (it's
	# always the final column)
	$result =~ s/\s+creation_time.*//g;

	# Strip leading and trailing whitespace
	$result =~ s/^\s+|\s+$//g;

	is($result, $expected_wal, $description);
}

sub discard_wal
{
	$db->safe_psql('postgres',
		qq/SELECT data FROM pg_logical_slot_get_changes('slot', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');/
	);
}

my $server_version = 0 + $db->safe_psql('postgres',
	"select current_setting('server_version_num')::int");

# stop background jobs because they can scribble in our WAL
run_queries(
	qq/
	SELECT _timescaledb_functions.stop_background_workers();
	/
);

run_queries(
	qq/
	CREATE TABLE metrics(time timestamptz, device_id bigint, value float8);
	SELECT create_hypertable('metrics', 'time');
	ALTER TABLE metrics REPLICA IDENTITY FULL;
	ALTER TABLE metrics SET (timescaledb.compress = true, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id');
	SELECT 'init' FROM pg_create_logical_replication_slot('slot', 'test_decoding');
	/
);

query_generates_wal(
	"insert into plain chunk",
	qq/INSERT INTO metrics VALUES ('2023-07-01T00:00:00Z', 1, 1.0);/,
	qq(table _timescaledb_catalog.dimension_slice: INSERT: id[integer]:1 dimension_id[integer]:1 range_start[bigint]:1687996800000000 range_end[bigint]:1688601600000000
table _timescaledb_catalog.chunk: INSERT: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:null dropped[boolean]:false status[integer]:0 osm_chunk[boolean]:false
table _timescaledb_catalog.chunk_constraint: INSERT: chunk_id[integer]:1 dimension_slice_id[integer]:1 constraint_name[name]:'constraint_1' hypertable_constraint_name[name]:null
table _timescaledb_catalog.chunk_index: INSERT: chunk_id[integer]:1 index_name[name]:'_hyper_1_1_chunk_metrics_time_idx' hypertable_id[integer]:1 hypertable_index_name[name]:'metrics_time_idx'
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:1)
);

query_generates_wal(
	"compress chunk",
	qq(SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(message: transactional: 1 prefix: ::timescaledb-compression-start, sz: 0 content:
table _timescaledb_catalog.chunk: INSERT: id[integer]:2 hypertable_id[integer]:2 schema_name[name]:'_timescaledb_internal' table_name[name]:'compress_hyper_2_2_chunk' compressed_chunk_id[integer]:null dropped[boolean]:false status[integer]:0 osm_chunk[boolean]:false
table _timescaledb_catalog.compression_settings: INSERT: relid[regclass]:'_timescaledb_internal.compress_hyper_2_2_chunk' segmentby[text[]]:'{device_id}' orderby[text[]]:'{time}' orderby_desc[boolean[]]:'{f}' orderby_nullsfirst[boolean[]]:'{f}'
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:2 dropped[boolean]:false status[integer]:1 osm_chunk[boolean]:false
table _timescaledb_internal.compress_hyper_2_2_chunk: INSERT: _ts_meta_count[integer]:1 device_id[bigint]:1 _ts_meta_min_1[timestamp with time zone]:'2023-06-30 17:00:00-07' _ts_meta_max_1[timestamp with time zone]:'2023-06-30 17:00:00-07' "time"[_timescaledb_internal.compressed_data]:'BAAAAqJgYhxAAAAComBiHEAAAAAAAQAAAAEAAAAAAAAADgAFRMDEOIAA' value[_timescaledb_internal.compressed_data]:'AwA/8AAAAAAAAAAAAAEAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAEAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAEGAAAAAAAAAAIAAAABAAAAAQAAAAAAAAAEAAAAAAAAAAoAAAABCgAAAAAAAAP/'
table _timescaledb_catalog.compression_chunk_size: INSERT: chunk_id[integer]:1 compressed_chunk_id[integer]:2 uncompressed_heap_size[bigint]:8192 uncompressed_toast_size[bigint]:0 uncompressed_index_size[bigint]:16384 compressed_heap_size[bigint]:16384 compressed_toast_size[bigint]:8192 compressed_index_size[bigint]:16384 numrows_pre_compression[bigint]:1 numrows_post_compression[bigint]:1 numrows_frozen_immediately[bigint]:1
message: transactional: 1 prefix: ::timescaledb-compression-end, sz: 0 content:)
);

query_generates_wal(
	"decompress chunk",
	qq(SELECT decompress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(message: transactional: 1 prefix: ::timescaledb-decompression-start, sz: 0 content:
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_catalog.compression_chunk_size: DELETE: chunk_id[integer]:1
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:null dropped[boolean]:false status[integer]:0 osm_chunk[boolean]:false
table _timescaledb_catalog.compression_settings: DELETE: relid[regclass]:'_timescaledb_internal.compress_hyper_2_2_chunk'
table _timescaledb_catalog.chunk: DELETE: id[integer]:2
message: transactional: 1 prefix: ::timescaledb-decompression-end, sz: 0 content:)
);

run_queries(
	qq(SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);)
);
discard_wal();

query_generates_wal(
	"recompress chunk (NOOP)",
	qq(SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(message: transactional: 1 prefix: ::timescaledb-compression-start, sz: 0 content:
message: transactional: 1 prefix: ::timescaledb-compression-end, sz: 0 content:),
);

query_generates_wal(
	"insert into uncompressed chunk",
	qq(INSERT INTO metrics VALUES ('2023-07-01T01:00:00Z', 1, 1.0);),
	qq(table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 18:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:3 dropped[boolean]:false status[integer]:9 osm_chunk[boolean]:false),
);

query_generates_wal(
	"recompress chunk",
	qq(SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(message: transactional: 1 prefix: ::timescaledb-compression-start, sz: 0 content:
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:3 dropped[boolean]:false status[integer]:1 osm_chunk[boolean]:false
table _timescaledb_internal._hyper_1_1_chunk: DELETE: "time"[timestamp with time zone]:'2023-06-30 18:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_internal.compress_hyper_2_3_chunk: DELETE: (no-tuple-data)
table _timescaledb_internal.compress_hyper_2_3_chunk: INSERT: _ts_meta_count[integer]:2 device_id[bigint]:1 _ts_meta_min_1[timestamp with time zone]:'2023-06-30 17:00:00-07' _ts_meta_max_1[timestamp with time zone]:'2023-06-30 18:00:00-07' "time"[_timescaledb_internal.compressed_data]:'BAAAAqJhOK/kAAAAAADWk6QAAAAAAgAAAAIAAAAAAAAA7gAFRMDEOIAAAAVEvxcRN/8=' value[_timescaledb_internal.compressed_data]:'AwA/8AAAAAAAAAAAAAIAAAABAAAAAAAAAAEAAAAAAAAAAwAAAAIAAAABAAAAAAAAAAEAAAAAAAAAAwAAAAEMAAAAAAAAD8IAAAACAAAAAQAAAAAAAAAEAAAAAAAAAAoAAAABCgAAAAAAAAP/'
message: transactional: 1 prefix: ::timescaledb-compression-end, sz: 0 content:)
);

query_generates_wal(
	"insert into uncompressed chunk 2",
	qq(INSERT INTO metrics VALUES ('2023-07-01T02:00:00Z', 1, 1.0);),
	qq(table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 19:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:3 dropped[boolean]:false status[integer]:9 osm_chunk[boolean]:false),
);

query_generates_wal(
	"update rows in uncompressed and compressed chunk",
	qq(UPDATE metrics SET value = 22 WHERE time IN ('2023-07-01T00:00:00Z', '2023-07-01T02:00:00Z');),
	qq(message: transactional: 1 prefix: ::timescaledb-decompression-start, sz: 0 content:
table _timescaledb_internal.compress_hyper_2_3_chunk: DELETE: (no-tuple-data)
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 18:00:00-07' device_id[bigint]:1 value[double precision]:1
message: transactional: 1 prefix: ::timescaledb-decompression-end, sz: 0 content:
table _timescaledb_internal._hyper_1_1_chunk: UPDATE: old-key: "time"[timestamp with time zone]:'2023-06-30 19:00:00-07' device_id[bigint]:1 value[double precision]:1 new-tuple: "time"[timestamp with time zone]:'2023-06-30 19:00:00-07' device_id[bigint]:1 value[double precision]:22
table _timescaledb_internal._hyper_1_1_chunk: UPDATE: old-key: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:1 new-tuple: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:22),
);

query_generates_wal(
	"compress chunk after update",
	qq(SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(message: transactional: 1 prefix: ::timescaledb-compression-start, sz: 0 content:
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:3 dropped[boolean]:false status[integer]:1 osm_chunk[boolean]:false
table _timescaledb_internal._hyper_1_1_chunk: DELETE: "time"[timestamp with time zone]:'2023-06-30 18:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_internal._hyper_1_1_chunk: DELETE: "time"[timestamp with time zone]:'2023-06-30 19:00:00-07' device_id[bigint]:1 value[double precision]:22
table _timescaledb_internal._hyper_1_1_chunk: DELETE: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:22
table _timescaledb_internal.compress_hyper_2_3_chunk: INSERT: _ts_meta_count[integer]:3 device_id[bigint]:1 _ts_meta_min_1[timestamp with time zone]:'2023-06-30 17:00:00-07' _ts_meta_max_1[timestamp with time zone]:'2023-06-30 19:00:00-07' "time"[_timescaledb_internal.compressed_data]:'BAAAAqJiD0OIAAAAAADWk6QAAAAAAwAAAAMAAAAAAAAB7gAFRMDEOIAAAAVEvxcRN/8AAAAAAAAAAA==' value[_timescaledb_internal.compressed_data]:'AwBANgAAAAAAAAAAAAMAAAABAAAAAAAAAAEAAAAAAAAABwAAAAMAAAABAAAAAAAAAAEAAAAAAAAABwAAAAESAAAAAAAAEEEAAAADAAAAAQAAAAAAAAAEAAAAAAAADu4AAAABKgAAA/4/+OAb'
message: transactional: 1 prefix: ::timescaledb-compression-end, sz: 0 content:)
);

query_generates_wal(
	"insert into uncompressed chunk 3",
	qq(INSERT INTO metrics VALUES ('2023-07-01T03:00:00Z', 1, 1.0);),
	qq(table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 20:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:3 dropped[boolean]:false status[integer]:9 osm_chunk[boolean]:false),
);

query_generates_wal(
	"delete row in compressed chunk",
	qq(DELETE FROM metrics WHERE time IN ('2023-07-01T00:00:00Z', '2023-07-01T03:00:00Z');),
	qq(message: transactional: 1 prefix: ::timescaledb-decompression-start, sz: 0 content:
table _timescaledb_internal.compress_hyper_2_3_chunk: DELETE: (no-tuple-data)
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:22
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 18:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 19:00:00-07' device_id[bigint]:1 value[double precision]:22
message: transactional: 1 prefix: ::timescaledb-decompression-end, sz: 0 content:
table _timescaledb_internal._hyper_1_1_chunk: DELETE: "time"[timestamp with time zone]:'2023-06-30 20:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_internal._hyper_1_1_chunk: DELETE: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:22)
);

# switch to PK on time for insert with PK decompression test
run_queries(
	qq/
    SELECT decompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
    DELETE FROM metrics;
    ALTER TABLE metrics SET (timescaledb.compress = false);
    ALTER TABLE metrics REPLICA IDENTITY DEFAULT;
    ALTER TABLE metrics ADD PRIMARY KEY (time);
    ALTER TABLE metrics SET (timescaledb.compress = true, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id');
    INSERT INTO metrics VALUES('2023-07-01T00:00:00Z', 1, 1.0), ('2023-07-01T12:00:00Z', 1, 2.0);
    SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
    /
);

discard_wal();

query_generates_wal(
	"insert into compressed chunk with pk forces decompression",
	qq/INSERT INTO metrics VALUES ('2023-07-01 00:00:00Z', 1, 5555) ON CONFLICT DO NOTHING;/,
	qq/table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:4 dropped[boolean]:false status[integer]:9 osm_chunk[boolean]:false/
);

# Disable marker generation through GUC
$db->append_conf('postgresql.conf',
	'timescaledb.enable_compression_wal_markers=false');
$db->reload();

query_generates_wal(
	"compress chunk after disabling markers",
	qq(SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:4 dropped[boolean]:false status[integer]:1 osm_chunk[boolean]:false)
);

query_generates_wal(
	"decompress chunk after disabling markers",
	qq(SELECT decompress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);),
	qq(table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-06-30 17:00:00-07' device_id[bigint]:1 value[double precision]:1
table _timescaledb_internal._hyper_1_1_chunk: INSERT: "time"[timestamp with time zone]:'2023-07-01 05:00:00-07' device_id[bigint]:1 value[double precision]:2
table _timescaledb_catalog.compression_chunk_size: DELETE: chunk_id[integer]:1
table _timescaledb_catalog.chunk: UPDATE: id[integer]:1 hypertable_id[integer]:1 schema_name[name]:'_timescaledb_internal' table_name[name]:'_hyper_1_1_chunk' compressed_chunk_id[integer]:null dropped[boolean]:false status[integer]:0 osm_chunk[boolean]:false
table _timescaledb_catalog.compression_settings: DELETE: relid[regclass]:'_timescaledb_internal.compress_hyper_3_4_chunk'
table _timescaledb_catalog.chunk: DELETE: id[integer]:4)
);

# re-enable background jobs
run_queries(
	qq/
	SELECT public.alter_job(id::integer, scheduled=>true) FROM _timescaledb_config.bgw_job;
	/
);

pass();

done_testing();
