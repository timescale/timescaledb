-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

/*
 * Note on testing: need a couple wrappers that pg_sleep in a loop to wait for changes
 * to appear in pg_stat_activity.
 * Further Note: PG 9.6 changed what appeared in pg_stat_activity, so the launcher doesn't actually show up. 
 * we can still test its interactions with its children, but can't test some of the things specific to the launcher. 
 * So we've added some bits about the version number as needed. 
 */

CREATE VIEW worker_counts as SELECT count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Launcher') as launcher,
count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = 'single') as single_scheduler,
count(*) filter (WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = 'single_2') as single_2_scheduler, 
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
