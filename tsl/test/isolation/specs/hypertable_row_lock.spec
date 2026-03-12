# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test explicit row locking (FOR UPDATE/FOR SHARE) on hypertables.
#
# When a transaction holds a row lock via SELECT FOR UPDATE/FOR SHARE,
# concurrent modifications to those rows must block until the lock is released.
#
# Lock compatibility matrix (relevant cases):
#   FOR UPDATE blocks: UPDATE, DELETE, FOR UPDATE, FOR NO KEY UPDATE
#   FOR SHARE blocks: UPDATE, DELETE, FOR UPDATE, FOR NO KEY UPDATE
#   FOR SHARE allows: FOR SHARE, FOR KEY SHARE
#
# This test verifies:
#   1. FOR UPDATE on hypertable blocks concurrent UPDATE
#   2. FOR UPDATE on hypertable blocks concurrent DELETE
#   3. FOR SHARE on hypertable blocks concurrent DELETE
#   4. Multi-chunk scenario: locks work across multiple chunks
#
# Each scenario compares plain table (reference) with hypertable.

setup {
  -- Plain table (reference implementation)
  CREATE TABLE plain (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT,
    PRIMARY KEY (time, device_id)
  );
  INSERT INTO plain VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0),
    ('2020-01-02 00:00:00+00', 1, 20.0),
    ('2020-01-03 00:00:00+00', 1, 30.0);

  -- Hypertable with multiple chunks (1 day interval)
  CREATE TABLE ht (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT,
    PRIMARY KEY (time, device_id)
  );
  SELECT create_hypertable('ht', 'time', chunk_time_interval => INTERVAL '1 day');
  INSERT INTO ht VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0),
    ('2020-01-02 00:00:00+00', 1, 20.0),
    ('2020-01-03 00:00:00+00', 1, 30.0);
}

teardown {
  DROP TABLE IF EXISTS plain, ht CASCADE;
}

# Session 1: Acquire row locks
session "s1"

step "s1_for_update_plain" {
  BEGIN;
  SELECT * FROM plain WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1 FOR UPDATE;
}

step "s1_for_update_ht" {
  BEGIN;
  SELECT * FROM ht WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1 FOR UPDATE;
}

step "s1_for_share_plain" {
  BEGIN;
  SELECT * FROM plain WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1 FOR SHARE;
}

step "s1_for_share_ht" {
  BEGIN;
  SELECT * FROM ht WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1 FOR SHARE;
}

# Lock multiple rows across chunks
step "s1_for_update_multi_ht" {
  BEGIN;
  SELECT * FROM ht WHERE device_id = 1 FOR UPDATE;
}

step "s1_commit" {
  COMMIT;
}

step "s1_rollback" {
  ROLLBACK;
}

# Session 2: Attempt concurrent modifications
session "s2"

step "s2_update_plain" {
  UPDATE plain SET value = 999.0 WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1;
}

step "s2_update_ht" {
  UPDATE ht SET value = 999.0 WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1;
}

step "s2_delete_plain" {
  DELETE FROM plain WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1;
}

step "s2_delete_ht" {
  DELETE FROM ht WHERE time = '2020-01-01 00:00:00+00' AND device_id = 1;
}

# Update a different row (in different chunk) - should NOT block
step "s2_update_other_ht" {
  UPDATE ht SET value = 888.0 WHERE time = '2020-01-02 00:00:00+00' AND device_id = 1;
}

step "s2_check_values_ht" {
  SELECT time, value FROM ht WHERE device_id = 1 ORDER BY time;
}

# Test 1: FOR UPDATE blocks UPDATE
# Plain table reference
permutation "s1_for_update_plain" "s2_update_plain" "s1_commit"
# Hypertable
permutation "s1_for_update_ht" "s2_update_ht" "s1_commit"

# Test 2: FOR UPDATE blocks DELETE
# Plain table reference
permutation "s1_for_update_plain" "s2_delete_plain" "s1_rollback"
# Hypertable
permutation "s1_for_update_ht" "s2_delete_ht" "s1_rollback"

# Test 3: FOR SHARE blocks DELETE
# Plain table reference
permutation "s1_for_share_plain" "s2_delete_plain" "s1_rollback"
# Hypertable
permutation "s1_for_share_ht" "s2_delete_ht" "s1_rollback"

# Test 4: FOR SHARE blocks UPDATE
# Plain table reference
permutation "s1_for_share_plain" "s2_update_plain" "s1_rollback"
# Hypertable
permutation "s1_for_share_ht" "s2_update_ht" "s1_rollback"

# Test 5: Multi-chunk FOR UPDATE - verify locks work across chunks
# Update to different chunk should NOT block while same-chunk update would
permutation "s1_for_update_ht" "s2_update_other_ht" "s1_commit" "s2_check_values_ht"

# Test 6: Multi-chunk FOR UPDATE - locking all rows blocks all updates
permutation "s1_for_update_multi_ht" "s2_update_other_ht" "s1_rollback"
