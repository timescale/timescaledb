-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Note on testing: need a couple wrappers that pg_sleep in a loop to wait for changes
-- to appear in pg_stat_activity.
-- Further Note: PG 9.6 changed what appeared in pg_stat_activity, so the launcher doesn't actually show up.
-- we can still test its interactions with its children, but can't test some of the things specific to the launcher.
-- So we've added some bits about the version number as needed.

CREATE VIEW worker_counts as SELECT count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Launcher') as launcher,
count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = :'TEST_DBNAME') as single_scheduler,
count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = :'TEST_DBNAME_2') as single_2_scheduler,
count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = 'template1') as template1_scheduler
FROM pg_stat_activity;

CREATE FUNCTION wait_worker_counts(launcher_ct INTEGER,  scheduler1_ct INTEGER, scheduler2_ct INTEGER, template1_ct INTEGER) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r INTEGER;
BEGIN
FOR i in 1..10
LOOP
SELECT COUNT(*) from worker_counts where (launcher = launcher_ct OR current_setting('server_version_num')::int < 100000)
	AND single_scheduler = scheduler1_ct AND single_2_scheduler = scheduler2_ct and template1_scheduler = template1_ct into r;
if(r < 1) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
	--We have the correct counts!
  RETURN TRUE;
END IF;
END LOOP;
RETURN FALSE;
END
$BODY$;

CREATE FUNCTION wait_for_bgw_scheduler(_datname NAME, _count INT DEFAULT 1, _ticks INT DEFAULT 10) RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  r INTEGER;
BEGIN
  FOR i in 1.._ticks
  LOOP
    SELECT count(*)
    FROM pg_stat_activity
    WHERE
      application_name = 'TimescaleDB Background Worker Scheduler' AND
      datname = _datname
    INTO r;

    IF(r <> _count) THEN
      PERFORM pg_sleep(0.1);
      PERFORM pg_stat_clear_snapshot();
    ELSE
      RETURN 'BGW Scheduler found.';
    END IF;

  END LOOP;

  RETURN 'BGW Scheduler NOT found.';

END
$BODY$;
