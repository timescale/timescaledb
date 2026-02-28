-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test OLD/NEW references in RETURNING clause (PG18+ feature)

CREATE TABLE ht_returning(
    time timestamptz NOT NULL,
    value int NOT NULL,
    UNIQUE(time)
);

SELECT * FROM create_hypertable('ht_returning', 'time');

-- Insert initial rows
INSERT INTO ht_returning(time, value) VALUES ('2024-01-01', 10);
INSERT INTO ht_returning(time, value) VALUES ('2024-01-02', 20);

-- Test 1: INSERT ON CONFLICT DO UPDATE with RETURNING OLD.col, NEW.col
-- This should show the old value (10) and the new value (100)
INSERT INTO ht_returning(time, value) VALUES ('2024-01-01', 100)
ON CONFLICT (time) DO UPDATE SET value = EXCLUDED.value
RETURNING OLD.value AS old_val, NEW.value AS new_val;

-- Test 2: INSERT ON CONFLICT DO UPDATE with RETURNING arithmetic on OLD/NEW
INSERT INTO ht_returning(time, value) VALUES ('2024-01-02', 50)
ON CONFLICT (time) DO UPDATE SET value = EXCLUDED.value
RETURNING OLD.value AS old_val, NEW.value AS new_val, NEW.value - OLD.value AS diff;

-- Test 3: Plain INSERT with RETURNING NEW.col (OLD should be NULL for fresh inserts)
INSERT INTO ht_returning(time, value) VALUES ('2024-01-03', 30)
RETURNING OLD.value AS old_val, NEW.value AS new_val;

-- Verify final state
SELECT * FROM ht_returning ORDER BY time;

-- Cleanup
DROP TABLE ht_returning;
