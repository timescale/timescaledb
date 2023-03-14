-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT current_setting('server_version_num')::int >=  130000 AS has_cagg_join_using \gset

\d+ cagg_joins_upgrade_test_with_realtime
SELECT * FROM cagg_joins_upgrade_test_with_realtime ORDER BY bucket;

\d+ cagg_joins_upgrade_test
SELECT * FROM cagg_joins_upgrade_test ORDER BY bucket;

\d+ cagg_joins_where
SELECT * FROM cagg_joins_where ORDER BY bucket;

\if :has_cagg_join_using
    \d+ cagg_joins_upgrade_test_with_realtime_using
    SELECT * FROM cagg_joins_upgrade_test_with_realtime_using ORDER BY bucket;

    \d+ cagg_joins_upgrade_test_using
    SELECT * FROM cagg_joins_upgrade_test_using ORDER BY bucket;

\endif

