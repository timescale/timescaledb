-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

\ir post.pre_insert.sql

-- INSERT data to create a new chunk after update or restore.
INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1, series_2) VALUES
(1258894000000000000, 'dev5', 2.2, 1, 2);

\ir post.post_insert.sql
