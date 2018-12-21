-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

SET timescaledb.disable_optimizations= 'on';
SET max_parallel_workers_per_gather = 0; -- Disable parallel for this test
\ir include/sql_query_results.sql
RESET max_parallel_workers_per_gather;
