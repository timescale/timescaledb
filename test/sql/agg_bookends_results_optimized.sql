-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

SET timescaledb.disable_optimizations= 'off';
\set PREFIX ''
\ir include/agg_bookends.sql
