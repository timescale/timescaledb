-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- Some test cases for DML with system-column references on a hypertable target.
\set PREFIX 'EXPLAIN (analyze, verbose, buffers off, costs off, timing off, summary off)'

CREATE TABLE ht_update_join(time timestamptz NOT NULL, val int);
SELECT create_hypertable('ht_update_join', 'time', chunk_time_interval => interval '1 month');
INSERT INTO ht_update_join VALUES ('2020-01-15', 1), ('2020-02-15', 2);
VACUUM FREEZE ANALYZE ht_update_join;

-- ctid self-join UPDATE
BEGIN;
:PREFIX
UPDATE ht_update_join a SET val = 0
FROM ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid
;
ROLLBACK;

-- WITH ... UPDATE ... RETURNING * nesting
BEGIN;
:PREFIX
WITH updated AS (
  UPDATE ht_update_join SET val = 99 WHERE time = '2020-01-15' RETURNING *
)
SELECT * FROM updated
;
ROLLBACK;

-- RETURNING ctid explicitly
BEGIN;
:PREFIX
UPDATE ht_update_join SET val = 98 WHERE time = '2020-01-15'
RETURNING ctid, time, val
;
ROLLBACK;

-- DELETE with ctid self-join
BEGIN;
:PREFIX
DELETE FROM ht_update_join a
USING ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid
;
ROLLBACK;

-- UPDATE with tableoid in join condition
BEGIN;
:PREFIX
UPDATE ht_update_join a SET val = 10
FROM ht_update_join b
WHERE a.time = b.time AND a.tableoid = b.tableoid
;
ROLLBACK;

-- UPDATE with ctid+tableoid in join condition
BEGIN;
:PREFIX
UPDATE ht_update_join a SET val = 20
FROM ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid AND a.tableoid = b.tableoid
;
ROLLBACK;

-- UPDATE with ctid in WHERE (not join): ctid used as qual
BEGIN;
:PREFIX UPDATE ht_update_join a SET val = 15 WHERE a.ctid = '(0,1)';
ROLLBACK;

-- UPDATE with xmin in WHERE: row identity must not strip xmin
BEGIN;
:PREFIX UPDATE ht_update_join SET val = 16 WHERE xmin::text::bigint > 0;
ROLLBACK;

-- UPDATE with xmin RETURNING: must survive
BEGIN;
:PREFIX
UPDATE ht_update_join SET val = 161
WHERE time = '2020-01-15'
RETURNING xmin, ctid, val
;
ROLLBACK;

-- UPDATE with xmax in WHERE (system column, not row-identity)
BEGIN;
:PREFIX UPDATE ht_update_join SET val = 162 WHERE xmax::text::bigint >= 0;
ROLLBACK;

-- UPDATE with cmin/cmax in WHERE
BEGIN;
:PREFIX
UPDATE ht_update_join SET val = 163
WHERE cmin::text::bigint >= 0 AND cmax::text::bigint >= 0
;
ROLLBACK;

-- UPDATE with whole-row reference in RETURNING
BEGIN;
:PREFIX
UPDATE ht_update_join SET val = 164
WHERE time = '2020-02-15'
RETURNING ht_update_join.*, ctid
;
ROLLBACK;

-- UPDATE that joins on ctid AND filters on xmin: ctid+tableoid get
-- the row-identity treatment, xmin must pass through unaffected
BEGIN;
:PREFIX
UPDATE ht_update_join a SET val = 165
FROM ht_update_join b
WHERE a.time = b.time
  AND a.ctid = b.ctid
  AND a.xmin::text::bigint > 0
;
ROLLBACK;

-- DELETE with xmin in qual
BEGIN;
:PREFIX DELETE FROM ht_update_join WHERE xmin::text::bigint > 0 AND val < 0;
ROLLBACK;

-- UPDATE with correlated EXISTS on ctid (semi-join from subquery pullup)
BEGIN;
:PREFIX
UPDATE ht_update_join SET val = 17
WHERE EXISTS (SELECT 1 FROM ht_update_join b
              WHERE b.ctid = ht_update_join.ctid AND b.time = ht_update_join.time)
;
ROLLBACK;

-- Plain UPDATE without join (negative control)
BEGIN;
:PREFIX UPDATE ht_update_join SET val = 30 WHERE time = '2020-01-15';
ROLLBACK;

-- DELETE with ctid+tableoid self-join
BEGIN;
:PREFIX
DELETE FROM ht_update_join a
USING ht_update_join b
WHERE a.time = b.time AND a.ctid = b.ctid AND a.tableoid = b.tableoid
;
ROLLBACK;

DROP TABLE ht_update_join;

-- UPDATE/DELETE on a hypertable with zero chunks.
CREATE TABLE ht_zero_chunks(time timestamptz NOT NULL, v int);
SELECT create_hypertable('ht_zero_chunks', 'time');
VACUUM FREEZE ANALYZE ht_zero_chunks;

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
VACUUM FREEZE ANALYZE ht_dummy;

:PREFIX UPDATE ht_dummy SET v = 99 WHERE 1 = 0;
:PREFIX DELETE FROM ht_dummy WHERE false;
:PREFIX UPDATE ht_dummy SET v = 99 WHERE time IS NULL;

:PREFIX
UPDATE ht_dummy SET v = 99 WHERE 1 = 0
RETURNING ctid, xmin, tableoid::regclass, *
;

DROP TABLE ht_dummy;

