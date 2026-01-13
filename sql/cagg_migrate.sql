-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions and procedures to migrate old continuous
-- aggregate format to the finals form (without partials).

-- Migrate a CAgg which is using the experimental time_bucket_ng function
-- into a CAgg using the regular time_bucket function
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS)
AS '@MODULE_PATHNAME@', 'ts_continuous_agg_migrate_to_time_bucket' LANGUAGE C;
