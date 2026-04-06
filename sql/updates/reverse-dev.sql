DROP FUNCTION IF EXISTS @extschema@.add_continuous_aggregate_column(REGCLASS, TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS @extschema@.drop_continuous_aggregate_column(REGCLASS, NAME, BOOLEAN);
