
DROP FUNCTION IF EXISTS detach_data_node(name,regclass,boolean,boolean);
DROP FUNCTION IF EXISTS distributed_exec;

DROP PROCEDURE IF EXISTS refresh_continuous_aggregate(regclass,"any","any");

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
