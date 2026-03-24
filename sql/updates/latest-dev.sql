
-- delete compression policies from materialization hypertables since those are built into refresh policies now
DELETE FROM _timescaledb_catalog.bgw_job j where proc_name = 'policy_compression' AND EXISTS(SELECT FROM _timescaledb_catalog.continuous_agg c WHERE c.mat_hypertable_id = j.hypertable_id);

