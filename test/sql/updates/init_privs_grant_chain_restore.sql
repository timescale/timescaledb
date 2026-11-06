-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE EXTENSION timescaledb VERSION :'TO_VERSION';
-- Recreate the valid initial baseline for the alternative-path fixture. The
-- chunk table uses the normal installation baseline, so its current INSERT
-- grants must be supplied by the dump rather than by this setup.
ALTER EXTENSION timescaledb DROP VIEW _timescaledb_config.bgw_job;
GRANT INSERT ON _timescaledb_config.bgw_job TO initacl_chain_middle WITH GRANT OPTION;
SET ROLE initacl_chain_middle;
GRANT INSERT ON _timescaledb_config.bgw_job TO initacl_chain_leaf;
RESET ROLE;
ALTER EXTENSION timescaledb ADD VIEW _timescaledb_config.bgw_job;
