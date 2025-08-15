-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests to make sure SkipScan NOT NULL mode is used correctly
-- All SkipScan modes shold be enabled
SELECT current_setting('timescaledb.enable_skipscan') as enable_skipscan,
       current_setting('timescaledb.enable_skipscan_for_distinct_aggregates') AS enable_dagg_skipscan,
       current_setting('timescaledb.enable_compressed_skipscan') AS enable_compressed_skipscan;

-- Base queries with regular NULL mode
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE order by dev DESC;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
reset client_min_messages;

-- Index quals except IS NULL discard NULLs
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev IS NOT NULL;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev IS NOT NULL;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev > 2;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev BETWEEN 1 AND 5;
reset client_min_messages;

-- next two filters produce small output for which we may select seqscan, don't want it
set enable_seqscan=0;
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev in (1,2,3);
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev IS NULL;
reset client_min_messages;

-- PG is smart enough to convert negation of IS NOT NULL to IS NULL
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE (dev IS NOT NULL) = false;
reset client_min_messages;

reset enable_seqscan;

-- Strict Index filters discard NULLs
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev * 2 < 8;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev * 2 < 8;
reset client_min_messages;

-- OR is non-strict
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev < 2 OR dev > 0;
reset client_min_messages;

-- Strict non-index filters on relation discard NULLs
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev + time < 100;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev + time < 100;
reset client_min_messages;

-- OR is non-strict
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev + time < 100 OR dev > 0;
reset client_min_messages;

-- Index qual on a non-skip key: skip key can be NULL
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev_name > 'device_1';
reset client_min_messages;

-- Index quals on several keys including skip key
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev_name > 'device_1' and dev > 2;
reset client_min_messages;

-- Index qual row comparison will filter out NULLs in skip key when skip key is a leading column
-- But for compressed chunks ROW is not pushed into index quals, it is treated as a filter and is not strict
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE row(dev,dev_name) > row(1,'device_1');
reset client_min_messages;

-- Satisfying (dev_name>'device_1') may allow NULLs into dev
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE row(dev_name,dev) > row('device_1',1);
reset client_min_messages;

-- Non-index qual row comparison: row comparison in general is not strict
set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE row(dev+1,dev_name) > row(1,'device_1');
reset client_min_messages;

-- Distinct column is declared NOT NULL
DELETE FROM :TABLE WHERE dev IS NULL;
ALTER TABLE :TABLE ALTER COLUMN dev SET NOT NULL;

set client_min_messages to DEBUG1;
:PREFIX SELECT DISTINCT dev FROM :TABLE;
reset client_min_messages;

set client_min_messages to DEBUG1;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
reset client_min_messages;
