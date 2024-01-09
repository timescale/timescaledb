-- Manually drop the following functions / procedures since 'OR REPLACE' is missing in 2.13.0
DROP PROCEDURE IF EXISTS _timescaledb_functions.repair_relation_acls();
DROP FUNCTION IF EXISTS _timescaledb_functions.makeaclitem(regrole, regrole, text, bool);
