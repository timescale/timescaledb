-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.pre_insert.sql

-- INSERT data to create a new chunk after update or restore.
INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1, series_2) VALUES
(1258894000000000000, 'dev5', 2.2, 1, 2);

\ir post.post_insert.sql
