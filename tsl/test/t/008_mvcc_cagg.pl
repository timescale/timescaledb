# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# This TAP test tests the MVCC behavior of the watermark function of the CAGGs
# It creates a hypertable, a CAGG, and a refresh policy. Afterward, it inserts
# into the hypertable and checks that all workers of a parallel query see the same
# watermark.

use strict;
use warnings;
use TimescaleNode;
use Data::Dumper;
use Test::More tests => 5;

# See debug output in 'tsl/test/tmp_check/log/regress_log_008_mvcc_cagg'
my $debug = 0;

# Should be enabled as soon as _timescaledb_internal.cagg_watermark is declared as parallel safe
my $check_for_parallel_plan = 0;

my $timescale_node = TimescaleNode->create('insert');

# Test 1 - Create table
my $result = $timescale_node->safe_psql(
	'postgres', q{
  CREATE TABLE adapter_metrics (
  time timestamptz NOT NULL,
  adapter_token text NOT NULL,
  dyno text NOT NULL,
  queue text NOT NULL,
  value double precision NOT NULL);
}
);
is($result, '', 'create table');

# Test 2 - Convert to hypertable
$result = $timescale_node->safe_psql(
	'postgres', q{
  SELECT FROM create_hypertable('adapter_metrics','time', chunk_time_interval => INTERVAL '5 seconds');
}
);
is($result, '', 'create hypertable');

# Test 3 - Create CAGG
$result = $timescale_node->safe_psql(
	'postgres', q{
  CREATE MATERIALIZED VIEW adapter_metrics_rollup WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
  SELECT
      time_bucket('00:00:05'::interval, adapter_metrics.time) AS bucket,
      avg(adapter_metrics.value) AS avg_value,
      max(adapter_metrics.value) AS max_value,
      count(*) AS count
  FROM adapter_metrics
  GROUP BY time_bucket('00:00:05'::interval, adapter_metrics.time)
  WITH DATA;
}
);
is($result, '', 'crete cagg');

# Test 4 - Create CAGG policy
$result = $timescale_node->safe_psql(
	'postgres', q{
  SELECT add_continuous_aggregate_policy('adapter_metrics_rollup',
    start_offset => INTERVAL '20 seconds',
    end_offset => NULL,
    schedule_interval => INTERVAL '1 seconds');
}
);
is($result, '1000', 'job id of the cagg');

# Test 5 - Test consistent CAGG watermarks
for (my $iteration = 0; $iteration < 10; $iteration++)
{
	# Insert data
	$timescale_node->safe_psql(
		'postgres', q{
     INSERT INTO adapter_metrics
     SELECT
     now(),
     random()::text AS dyno,
     random()::text AS queue,
     random()::text AS adapter_token,
     value
     FROM
     generate_series(1, 100, 1) AS g2(value);
   }
	);

	# Perform selects while the CAGG update is running
	for (my $query = 0; $query < 10; $query++)
	{
		# Encourage the use of parallel workers
		my $sql = q{
           SET force_parallel_mode = 1;
           SET enable_bitmapscan = 0;
           SET parallel_setup_cost = 0;
           SET parallel_tuple_cost = 0;
           SET parallel_tuple_cost = 0;
           SET client_min_messages TO DEBUG5;
           SELECT * FROM adapter_metrics_rollup WHERE adapter_metrics_rollup.bucket > now() - INTERVAL '30 seconds';
        };
		my ($stdout, $stderr);

		if ($debug == 1)
		{
			print "===== New query\n";
		}

		# To be able to access stderr, use psql instead of safe_psql
		$timescale_node->psql(
			'postgres', $sql,
			stdout        => \$stdout,
			stderr        => \$stderr,
			on_error_die  => 1,
			on_error_stop => 1);

		# Ensure all parallel workers have seen the same watermark
		my @log_lines       = split "\n", $stderr;
		my $seen_watermarks = 0;
		my $cagg_id         = -1;
		my $watermark       = -1;

		# Check that all workers have seen the same watermark
		foreach (@log_lines)
		{
			if (/watermark for continuous aggregate .+'(\d+)' is: (\d+)/)
			{
				my $current_cagg      = $1;
				my $current_watermark = $2;

				if ($debug == 1)
				{
					print $_ . "\n";
				}

				if ($watermark == -1)
				{
					$watermark = $current_watermark;
					$cagg_id   = $current_cagg;
				}
				elsif ($watermark != $current_watermark)
				{
					diag(
						"Some workers have read the watermark $watermark, one worker read the watermark $current_watermark\n"
					);
					fail();
					BAIL_OUT();
				}
				elsif ($cagg_id != $current_cagg)
				{
					diag(
						"Got watermarks for differnt CAGGs, this test can not handle this ($cagg_id / $current_cagg)\n"
					);
					fail();
					BAIL_OUT();
				}

				$seen_watermarks++;
			}
		}

		if ($check_for_parallel_plan == 1 && $seen_watermarks < 2)
		{
			diag(
				"Got watermark only from one worker, was a parallel plan used?"
			);
			fail();
			BAIL_OUT();
		}
	}
}

# No errors before? Let test 5 pass
pass();

done_testing();

1;
