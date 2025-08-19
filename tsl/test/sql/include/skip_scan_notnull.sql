-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Base queries with regular NULL mode
:PREFIX SELECT DISTINCT dev FROM :TABLE;

:PREFIX SELECT DISTINCT dev FROM :TABLE order by dev DESC;

-- Index quals except IS NULL discard NULLs
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev IS NOT NULL;

:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev > 2;

:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev BETWEEN 1 AND 5;

-- next two filters produce small output for which we may select seqscan, don't want it
set enable_seqscan=0;
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev in (1,2,3);

:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev IS NULL;

-- PG is smart enough to convert negation of IS NOT NULL to IS NULL index condition
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE (dev IS NOT NULL) = false;

-- Complex expressions over IS NOT NULL are non-strict filters
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE ((dev is null) = false)::int + 1 > 1;
reset enable_seqscan;

-- Strict Index filters discard NULLs
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev * 2 < 8;

-- OR is non-strict
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev < 2 OR dev > 0;
-- Coalesce is non-strict
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE coalesce(dev,-1) < 1;

-- Strict non-index filters on relation discard NULLs
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev + time < 100;

-- OR is non-strict
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev + time < 100 OR dev > 0;
-- Coalesce is non-strict
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE coalesce(dev,20) + time < 100;

-- If at least one filter is strict then it filters out NULLs
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE coalesce(dev,20) + time < 100 and dev + time < 100;

-- Index qual on a non-skip key: skip key can be NULL
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev_name > 'device_1';

-- Index quals on several keys including skip key
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE dev_name > 'device_1' and dev > 2;

-- Index qual row comparison will filter out NULLs in skip key when skip key is a leading column
-- But for compressed chunks ROW is not pushed into index quals, it is treated as a filter and is not strict
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE row(dev,dev_name) > row(1,'device_1');

-- Satisfying (dev_name>'device_1') may allow NULLs into dev
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE row(dev_name,dev) > row('device_1',1);

-- Non-index qual row comparison: row comparison in general is not strict
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE row(dev+1,dev_name) > row(1,'device_1');

-- Different SkipScan column
:PREFIX SELECT DISTINCT dev_name FROM :TABLE WHERE dev=1 and dev_name IS NOT NULL;

-- Boolean and NOT NULL constraint tests

-- Boolean column index qual is strict
:PREFIX SELECT DISTINCT b FROM skip_scan_b WHERE b;
:PREFIX SELECT DISTINCT b FROM skip_scan_b WHERE b != true;
-- Boolean column predicates could be non-strict filters, below filter can accept NULLs
:PREFIX SELECT DISTINCT b FROM skip_scan_b WHERE b IS NOT true;

-- dev is declared NOT NULL
:PREFIX SELECT DISTINCT dev FROM skip_scan_nn;
