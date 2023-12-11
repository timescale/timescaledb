# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Test::More;

plan skip_all => "PostgreSQL version < 14" if $ENV{PG_VERSION_MAJOR} < 14;

# This test checks the creation of logical replication messages
# used to mark the start and end of inserts happening as a result
# of a (partial) decompression.

# Publishing node
my $publisher =
  TimescaleNode->create('publisher', allows_streaming => 'logical');

# Subscribing node
my $subscriber =
  TimescaleNode->create('subscriber', allows_streaming => 'logical');

# Setup test structures
$publisher->safe_psql(
	'postgres',
	qq(
        CREATE TABLE test (ts timestamptz NOT NULL PRIMARY KEY , val INT);
        SELECT create_hypertable('test', 'ts', chunk_time_interval := INTERVAL '1day');
    )
);

# To kick off replication we need to fake the setup of a hypertable
$subscriber->safe_psql('postgres',
	"CREATE TABLE _timescaledb_internal._hyper_1_1_chunk (ts timestamptz NOT NULL PRIMARY KEY , val INT)"
);

# Initial data insert and preparation of the internal chunk tables
$publisher->safe_psql(
	'postgres',
	qq(
        INSERT INTO test
        SELECT s.s, (random() * 100)::INT
        FROM generate_series('2023-01-01'::timestamptz, '2023-01-02'::timestamptz, INTERVAL '3 hour') s;
    )
);

# Setup logical replication
my $publisher_connstr = $publisher->connstr . ' dbname=postgres';
$publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE _timescaledb_internal._hyper_1_1_chunk"
);
$subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (binary = true)"
);

# Wait for catchup and disable consumption of additional messages
$publisher->wait_for_catchup('tap_sub');
$subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION tap_sub DISABLE");
$publisher->poll_query_until(
	'postgres',
	"SELECT COUNT(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = 'tap_sub' AND active='f'",
	1);

# Enable marker generation through GUC
$publisher->append_conf('postgresql.conf',
	'timescaledb.enable_decompression_logrep_markers=true');
$publisher->reload();

# Compress chunks and consume replication stream explicitly
$publisher->safe_psql(
	'postgres',
	qq(
        ALTER TABLE test SET (timescaledb.compress);
        SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE);
    )
);
$publisher->safe_psql(
	'postgres',
	qq(
        SELECT pg_logical_slot_get_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        );
    )
);

# Create a new entry which forces a decompression to happen
$publisher->safe_psql('postgres',
	"INSERT INTO test VALUES ('2023-01-01 00:10:00', 5555)");

# Retrieve the replication log messages
my $result = $publisher->safe_psql(
	'postgres',
	qq(
        SELECT get_byte(data, 0)
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        );
    )
);

# Test: BEGIN, MESSAGE (start marker), RELATION, ... INSERT (decompression inserts x6) ..., MESSAGE (end marker), INSERT, COMMIT
is( $result,
	qq(66
77
82
73
73
73
73
73
73
77
73
67),
	'messages on slot meet expectation <<BEGIN, MESSAGE (start marker), RELATION, ... INSERT (decompression inserts x6) ..., MESSAGE (end marker), INSERT, COMMIT>>'
);

# Get initial message entry
$result = $publisher->safe_psql(
	'postgres',
	qq(
        SELECT get_byte(data, 1), encode(substr(data, 11, 33), 'escape')
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        )
        OFFSET 1 LIMIT 1;
    )
);
is( $result,
	qq(1|::timescaledb-decompression-start),
	'first entry is decompression marker start message');

# Get second message entry
$result = $publisher->safe_psql(
	'postgres',
	qq(
        SELECT get_byte(data, 1), encode(substr(data, 11, 31), 'escape')
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        )
        OFFSET 9 LIMIT 1;
    )
);
is( $result,
	qq(1|::timescaledb-decompression-end),
	'10th entry is decompression marker end message');

# Get last insert entry to check it is the user executed insert (and value is 5555 or 35353535 in hex)
$result = $publisher->safe_psql(
	'postgres',
	qq(
        SELECT get_byte(data, 0), encode(substring(data from 41 for 44), 'hex')
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        )
        OFFSET 10 LIMIT 1;
    )
);
is($result, qq(73|35353535), '11th entry is an insert message');

# Disable marker generation through GUC
$publisher->append_conf('postgresql.conf',
	'timescaledb.enable_decompression_logrep_markers=false');
$publisher->reload();

# Compress chunks and consume replication stream explicitly
$publisher->safe_psql('postgres',
	"CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk'::regclass, TRUE)"
);
$publisher->safe_psql(
	'postgres',
	qq(
        SELECT pg_logical_slot_get_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        );
    )
);

# Create a new entry which forces a decompression to happen
$publisher->safe_psql('postgres',
	"INSERT INTO test VALUES ('2023-01-01 00:11:00', 5555)");

# Retrieve the replication log messages
$result = $publisher->safe_psql(
	'postgres',
	qq(
        SELECT get_byte(data, 0)
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1',
            'publication_names', 'tap_pub',
            'messages', 'true'
        );
    )
);

# Test: BEGIN, RELATION, ... INSERT (decompression inserts x7) ..., INSERT, COMMIT
is( $result,
	qq(66
82
73
73
73
73
73
73
73
73
67),
	'messages on slot meet expectation <<BEGIN, RELATION, ... INSERT (decompression inserts x7) ..., INSERT, COMMIT>>'
);

done_testing();
