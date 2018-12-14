-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

\set ON_ERROR_STOP 0
SELECT locf(1);
SELECT interpolate(1);
SELECT time_bucket_gapfill(1,1,1,1);
\set ON_ERROR_STOP 1
