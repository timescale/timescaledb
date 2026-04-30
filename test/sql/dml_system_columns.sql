-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- Some test cases for DML with system-column references on a hypertable target.
\set PREFIX 'EXPLAIN (analyze, verbose, buffers off, costs off, timing off, summary off)'

CREATE TABLE ht_update_join(time timestamptz NOT NULL, val int);
SELECT create_hypertable('ht_update_join', 'time', chunk_time_interval => interval '1 month');
INSERT INTO ht_update_join VALUES ('2020-01-15', 1), ('2020-02-15', 2);

-- ctid self-join UPDATE
:PREFIX
UPDATE ht_update_join a SET val = 0
FROM ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid
;

-- WITH ... UPDATE ... RETURNING * nesting
:PREFIX
WITH updated AS (
  UPDATE ht_update_join SET val = 99 WHERE time = '2020-01-15' RETURNING *
)
SELECT * FROM updated
;

-- RETURNING ctid explicitly
:PREFIX
UPDATE ht_update_join SET val = 98 WHERE time = '2020-01-15'
RETURNING ctid, time, val
;

-- DELETE with ctid self-join
:PREFIX
DELETE FROM ht_update_join a
USING ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid
;

-- Re-insert for further tests
INSERT INTO ht_update_join VALUES ('2020-01-15', 1), ('2020-02-15', 2);

-- UPDATE with tableoid in join condition
:PREFIX
UPDATE ht_update_join a SET val = 10
FROM ht_update_join b
WHERE a.time = b.time AND a.tableoid = b.tableoid
;

-- UPDATE with ctid+tableoid in join condition
:PREFIX
UPDATE ht_update_join a SET val = 20
FROM ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid AND a.tableoid = b.tableoid
;

-- UPDATE with ctid in WHERE (not join): ctid used as qual
:PREFIX UPDATE ht_update_join a SET val = 15 WHERE a.ctid = '(0,1)';

-- UPDATE with xmin in WHERE: row identity must not strip xmin
:PREFIX UPDATE ht_update_join SET val = 16 WHERE xmin::text::bigint > 0;

-- UPDATE with xmin RETURNING: must survive
:PREFIX
UPDATE ht_update_join SET val = 161
WHERE time = '2020-01-15'
RETURNING xmin, ctid, val
;

-- UPDATE with xmax in WHERE (system column, not row-identity)
:PREFIX UPDATE ht_update_join SET val = 162 WHERE xmax::text::bigint >= 0;

-- UPDATE with cmin/cmax in WHERE
:PREFIX
UPDATE ht_update_join SET val = 163
WHERE cmin::text::bigint >= 0 AND cmax::text::bigint >= 0
;

-- UPDATE with whole-row reference in RETURNING
:PREFIX
UPDATE ht_update_join SET val = 164
WHERE time = '2020-02-15'
RETURNING ht_update_join.*, ctid
;

-- UPDATE that joins on ctid AND filters on xmin: ctid+tableoid get
-- the row-identity treatment, xmin must pass through unaffected
:PREFIX
UPDATE ht_update_join a SET val = 165
FROM ht_update_join b
WHERE a.time = b.time
  AND a.ctid = b.ctid
  AND a.xmin::text::bigint > 0
;

-- DELETE with xmin in qual
:PREFIX DELETE FROM ht_update_join WHERE xmin::text::bigint > 0 AND val < 0;

-- UPDATE with correlated EXISTS on ctid (semi-join from subquery pullup)
:PREFIX
UPDATE ht_update_join SET val = 17
WHERE EXISTS (SELECT 1 FROM ht_update_join b
              WHERE b.ctid = ht_update_join.ctid AND b.time = ht_update_join.time)
;

-- Plain UPDATE without join (negative control)
:PREFIX UPDATE ht_update_join SET val = 30 WHERE time = '2020-01-15';

-- DELETE with ctid+tableoid self-join
:PREFIX
DELETE FROM ht_update_join a
USING ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid AND a.tableoid = b.tableoid
;

DROP TABLE ht_update_join;

-- UPDATE/DELETE on a hypertable with zero chunks.
CREATE TABLE ht_zero_chunks(time timestamptz NOT NULL, v int);
SELECT create_hypertable('ht_zero_chunks', 'time');

:PREFIX UPDATE ht_zero_chunks SET v = 1 WHERE time = '2020-01-15';
:PREFIX DELETE FROM ht_zero_chunks WHERE time = '2020-01-15';
:PREFIX UPDATE ht_zero_chunks SET v = 1 RETURNING ctid, xmin, tableoid::regclass, *;
:PREFIX DELETE FROM ht_zero_chunks RETURNING ctid, xmin, tableoid::regclass, *;

DROP TABLE ht_zero_chunks;

-- UPDATE/DELETE on a hypertable that becomes a dummy rel due to
-- contradictory quals.
CREATE TABLE ht_dummy(time timestamptz NOT NULL, v int);
SELECT create_hypertable('ht_dummy', 'time', chunk_time_interval => interval '1 month');
INSERT INTO ht_dummy VALUES ('2020-01-15', 1);

:PREFIX UPDATE ht_dummy SET v = 99 WHERE 1 = 0;
:PREFIX DELETE FROM ht_dummy WHERE false;
:PREFIX UPDATE ht_dummy SET v = 99 WHERE time IS NULL;

:PREFIX
UPDATE ht_dummy SET v = 99 WHERE 1 = 0
RETURNING ctid, xmin, tableoid::regclass, *
;

DROP TABLE ht_dummy;

