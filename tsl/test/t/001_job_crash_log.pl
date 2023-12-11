# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

use strict;
use warnings;
use TimescaleNode;
use Test::More tests => 5;

# This test checks that a job crash is reported in the job errors table
# (that is, a record gets inserted into the table _timescaledb_internal.job_errors).
# We cannot do that with a regression test because the server will not recover after
# a crash in that case

my $node = TimescaleNode->create();

# by default PostgresNode doesn't doesn't restart after a crash
# taken from 013_crash_restart.pl
$node->safe_psql(
	'postgres',
	q[ALTER SYSTEM SET restart_after_crash = 1;
				   ALTER SYSTEM SET log_connections = 1;
				   SELECT pg_reload_conf();]);

# create proc to run as job
my $query = <<'END_OF_SQL';
CREATE OR REPLACE PROCEDURE custom_proc_sleep60(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
 RAISE NOTICE 'im about to sleep for 60 seconds, plenty of time for you to kill me';
 perform pg_sleep(60);
END
$$;
END_OF_SQL

my $ret = $node->safe_psql('postgres', "$query");

my $query_add =
  q[select add_job('custom_proc_sleep60', '5 minutes', initial_start => now())];
my $jobid = $node->safe_psql('postgres', "$query_add");
is($jobid, '1000', 'job was added');

my $query_pid_exists = <<"END_OF_QUERY";
select count(*) from pg_stat_activity
where application_name like 'User-Defined Action%'
   and query like '%custom_proc_sleep60%'
END_OF_QUERY
# select the pid of this job in order to kill it
my $query_pid = <<"END_OF_QUERY";
select pid from pg_stat_activity
where application_name like 'User-Defined Action%'
   and query like '%custom_proc_sleep60%'
END_OF_QUERY

ok($node->poll_query_until('postgres', "$query_pid_exists", '1'));

my $pid = $node->safe_psql('postgres', "$query_pid");
isnt($pid, "", "check the pid is not null");
# now kill the one backend
my $int_pid = int($pid);
kill 9, $int_pid;

# Wait till server restarts
is($node->poll_query_until('postgres', 'SELECT 1', '1'),
	1, "reconnected after SIGQUIT");

my $errlog = $node->safe_psql('postgres',
	'select count(*) from _timescaledb_internal.job_errors where job_id = 1000 and pid is null'
);
is($errlog, "1", "there is a row for the crash in the error log");

done_testing();
