# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test FOR KEY SHARE locking during FK constraint enforcement.
#
# When inserting into a table with an FK reference, PostgreSQL runs an FK check
# query that acquires FOR KEY SHARE locks on the referenced rows. This lock must
# block concurrent DELETE/UPDATE of the referenced row until the FK-inserting
# transaction commits.
#
# Without proper locking, a race condition can occur:
#   1. S1: INSERT into FK table, FK check finds referenced row (lock not acquired)
#   2. S2: DELETE from PK table succeeds (no lock blocking it)
#   3. S1: INSERT commits
#   4. Result: FK violation - orphan row in FK table
#
# This test compares:
#   1. Plain table - Reference implementation, correct behavior
#   2. Hypertable - TimescaleDB partitioning
#
# Expected behavior: DELETE should block while INSERT is in progress, then fail
# with FK violation error after INSERT commits.

setup {
  -- 1. Plain table (reference)
  CREATE TABLE metrics_plain (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT,
    PRIMARY KEY (time, device_id)
  );
  INSERT INTO metrics_plain VALUES ('2020-01-01 12:00', 1, 100.0);

  CREATE TABLE events_plain (
    metric_time TIMESTAMPTZ NOT NULL,
    metric_device INT NOT NULL,
    event_data TEXT,
    CONSTRAINT fk_plain FOREIGN KEY (metric_time, metric_device) REFERENCES metrics_plain(time, device_id)
  );

  -- 2. Hypertable
  CREATE TABLE metrics_ht (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT,
    PRIMARY KEY (time, device_id)
  );
  SELECT create_hypertable('metrics_ht', 'time', chunk_time_interval => INTERVAL '1 day');
  INSERT INTO metrics_ht VALUES ('2020-01-01 12:00', 1, 100.0);

  CREATE TABLE events_ht (
    metric_time TIMESTAMPTZ NOT NULL,
    metric_device INT NOT NULL,
    event_data TEXT,
    CONSTRAINT fk_ht FOREIGN KEY (metric_time, metric_device) REFERENCES metrics_ht(time, device_id)
  );
}

teardown {
  DROP TABLE IF EXISTS events_plain, metrics_plain CASCADE;
  DROP TABLE IF EXISTS events_ht, metrics_ht CASCADE;
}

# Session 1: Insert into FK table (triggers FOR KEY SHARE on referenced row)
session "s1"

step "s1_insert_plain" {
  BEGIN;
  INSERT INTO events_plain (metric_time, metric_device, event_data)
    VALUES ('2020-01-01 12:00', 1, 'test event');
}

step "s1_insert_ht" {
  BEGIN;
  INSERT INTO events_ht (metric_time, metric_device, event_data)
    VALUES ('2020-01-01 12:00', 1, 'test event');
}

step "s1_commit" {
  COMMIT;
}

# Session 2: Attempt to delete the referenced row
session "s2"

step "s2_delete_plain" {
  DELETE FROM metrics_plain WHERE time = '2020-01-01 12:00' AND device_id = 1;
}

step "s2_delete_ht" {
  DELETE FROM metrics_ht WHERE time = '2020-01-01 12:00' AND device_id = 1;
}

step "s2_check_orphans_ht" {
  SELECT count(*) AS orphans FROM events_ht
  WHERE (metric_time, metric_device) NOT IN (SELECT time, device_id FROM metrics_ht);
}

# Plain table: DELETE should block, then fail with FK violation
# Expected: s2_delete_plain shows "<waiting ...>", then ERROR after s1_commit
permutation "s1_insert_plain" "s2_delete_plain" "s1_commit"

# Hypertable: DELETE should block, then fail with FK violation
# Expected: s2_delete_ht shows "<waiting ...>", then ERROR after s1_commit
permutation "s1_insert_ht" "s2_delete_ht" "s1_commit" "s2_check_orphans_ht"
